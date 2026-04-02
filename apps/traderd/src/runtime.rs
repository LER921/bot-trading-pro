use crate::{HealthTracker, RuntimeStateMachine};
use accounting::{AccountingService, InMemoryAccountingService};
use anyhow::{bail, Context, Result};
use common::{Decimal, now_utc};
use config_loader::AppConfig;
use domain::{
    ExecutionReport, HealthState, InventorySnapshot, MarketSnapshot, OpenOrder, RuntimeState,
    StrategyContext, Symbol, SymbolBudget, SystemHealth,
};
use execution::{BinanceExecutionEngine, ExecutionEngine};
use exchange_binance_spot::{
    BinanceBootstrapState, BinanceExecutionEvent, BinanceMarketEvent, BinanceSpotGateway,
    BinanceUserStreamEvent, MarketStreamHandle, StreamKind, StreamStatus, UserStreamHandle,
};
use features::{FeatureEngine, SimpleFeatureEngine};
use market_data::{InMemoryMarketDataService, MarketDataService};
use portfolio::{InMemoryPortfolioService, PortfolioService};
use regime::{ExecutionRegimeDetector, RegimeDetector};
use risk::{RiskContext, RiskManager, StrictRiskManager};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use storage::{MemoryStorage, StorageEngine};
use strategy_coordinator::DefaultStrategyCoordinator;
use telemetry::{TelemetrySink, TracingTelemetry};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{info, warn};

const LOOP_INTERVAL_MS: u64 = 1_000;

#[derive(Debug)]
pub struct TraderRuntime<G>
where
    G: BinanceSpotGateway + Clone + Send + Sync + 'static,
{
    config: AppConfig,
    symbols: Vec<Symbol>,
    gateway: G,
    state_machine: RuntimeStateMachine,
    health_tracker: HealthTracker,
    market_data: InMemoryMarketDataService,
    features: SimpleFeatureEngine,
    regime_detector: ExecutionRegimeDetector,
    strategy_coordinator: DefaultStrategyCoordinator,
    risk_manager: StrictRiskManager,
    execution: BinanceExecutionEngine<G>,
    portfolio: InMemoryPortfolioService,
    accounting: InMemoryAccountingService,
    storage: MemoryStorage,
    telemetry: TracingTelemetry,
    seen_fills: HashSet<String>,
    market_stream: Option<MarketStreamHandle>,
    user_stream: Option<UserStreamHandle>,
}

impl<G> TraderRuntime<G>
where
    G: BinanceSpotGateway + Clone + Send + Sync + 'static,
{
    pub fn new(config: AppConfig, gateway: G) -> Self {
        let symbols = config.enabled_symbols();
        let budgets = config
            .pairs
            .pairs
            .iter()
            .filter(|pair| pair.enabled)
            .map(|pair| SymbolBudget {
                symbol: pair.symbol,
                max_quote_notional: pair.max_quote_notional,
                reserved_quote_notional: Decimal::ZERO,
            })
            .collect::<Vec<_>>();

        Self {
            risk_manager: StrictRiskManager::new(config.risk_limits()),
            execution: BinanceExecutionEngine::new(gateway.clone(), symbols.clone()),
            portfolio: InMemoryPortfolioService::new(budgets),
            config,
            symbols,
            gateway,
            state_machine: RuntimeStateMachine::new(RuntimeState::Bootstrap, "runtime initialized"),
            health_tracker: HealthTracker::default(),
            market_data: InMemoryMarketDataService::default(),
            features: SimpleFeatureEngine,
            regime_detector: ExecutionRegimeDetector::default(),
            strategy_coordinator: DefaultStrategyCoordinator::default(),
            accounting: InMemoryAccountingService::default(),
            storage: MemoryStorage::default(),
            telemetry: TracingTelemetry,
            seen_fills: HashSet::new(),
            market_stream: None,
            user_stream: None,
        }
    }

    pub fn state(&self) -> RuntimeState {
        self.state_machine.current_state()
    }

    pub fn state_snapshot(&self) -> domain::RuntimeSnapshot {
        self.state_machine.snapshot()
    }

    pub fn health(&self) -> SystemHealth {
        self.health_tracker.snapshot(
            &self.symbols,
            self.config.risk.stale_market_data_ms,
            self.config.risk.stale_account_events_ms,
            self.config.risk.max_clock_drift_ms,
        )
    }

    pub async fn bootstrap(&mut self) -> Result<()> {
        self.transition(RuntimeState::Bootstrap, "starting bootstrap").await?;
        if self.symbols.is_empty() {
            self.transition(RuntimeState::RiskOff, "no enabled symbols").await?;
            bail!("bootstrap failed: no enabled symbols");
        }

        self.gateway.ping_rest().await?;
        self.health_tracker.record_rest_success(now_utc());

        let exchange_time = self.gateway.sync_clock().await?;
        let drift_ms = self.health_tracker.record_clock_sample(exchange_time);
        info!(drift_ms, "exchange clock synchronized");

        let bootstrap = self
            .gateway
            .fetch_bootstrap_state(&self.symbols)
            .await
            .context("failed to fetch bootstrap state")?;
        self.apply_bootstrap_state(&bootstrap).await?;

        let symbols = self.symbols.clone();
        for symbol in symbols {
            let orderbook = self.gateway.fetch_orderbook_snapshot(symbol, 20).await?;
            let observed_at = orderbook.observed_at;
            self.market_data.apply_orderbook_snapshot(orderbook).await?;
            self.health_tracker.record_orderbook(symbol, observed_at);

            if let Some(trade) = self
                .gateway
                .fetch_recent_trades(symbol, 1)
                .await?
                .into_iter()
                .last()
            {
                self.health_tracker.record_trade(symbol, trade.event_time);
                self.market_data.apply_trade(trade).await?;
            }
        }

        self.transition(RuntimeState::Reconciling, "bootstrap data loaded").await?;

        if self.config.runtime.bootstrap_cancel_open_orders {
            let open_orders = self.execution.open_orders().await;
            if !open_orders.is_empty() {
                warn!(count = open_orders.len(), "canceling carried open orders during bootstrap");
                self.execution.cancel_all(None).await?;
            }
        }

        let reconciled_orders = self.execution.reconcile().await?;
        self.assert_bootstrap_coherent(&reconciled_orders, drift_ms).await?;
        self.start_live_streams().await?;

        self.transition(RuntimeState::Ready, "bootstrap and reconciliation completed")
            .await?;

        if self.config.live.trading_enabled && !self.config.live.start_in_paused_mode {
            self.transition(RuntimeState::Trading, "live trading enabled").await?;
        } else {
            self.transition(RuntimeState::Paused, "waiting for explicit trading enable").await?;
        }

        Ok(())
    }

    pub async fn run_cycle(&mut self) -> Result<usize> {
        if self.state() == RuntimeState::Shutdown {
            return Ok(0);
        }

        self.refresh_health().await;
        self.ingest_stream_statuses().await?;
        self.ingest_live_events().await?;
        self.ensure_market_data_resynced().await?;

        let fallback_active = self.should_enable_fallback().await;
        self.health_tracker.set_fallback_active(fallback_active);
        if fallback_active {
            warn!("websocket stream degraded, activating REST fallback");
            self.ingest_market_events().await?;
            self.ingest_account_events().await?;
            self.execution.reconcile().await?;
        }

        let health = self.health();
        self.telemetry.emit_health(&health).await?;

        self.align_runtime_state_with_health(&health).await?;

        let account = self
            .portfolio
            .account_snapshot()
            .await
            .context("missing account snapshot before pipeline evaluation")?;
        let open_orders = self.execution.open_orders().await;
        let inventory_map = self.collect_inventory().await;
        let mid_prices = self.collect_mid_prices().await;
        let risk_context = RiskContext {
            account: account.clone(),
            health: health.clone(),
            runtime_state: self.state(),
            inventory: inventory_map.clone(),
            open_orders: open_orders.clone(),
            mid_prices,
        };

        let mut placed_reports = 0usize;
        let symbols = self.symbols.clone();
        for symbol in symbols {
            let Some(snapshot) = self.market_data.snapshot(symbol).await else {
                continue;
            };

            let Some(pair) = self.config.pair(symbol) else {
                continue;
            };

            let feature_snapshot = self.features.compute(&snapshot).await?;
            let regime = self
                .regime_detector
                .detect(symbol, &feature_snapshot, &health)
                .await?;
            let inventory = inventory_map
                .get(&symbol)
                .cloned()
                .unwrap_or_else(|| empty_inventory(symbol));

            if let Some(best) = &snapshot.best_bid_ask {
                let mark_price = (best.bid_price + best.ask_price) / Decimal::from(2u32);
                self.portfolio.apply_mark_price(symbol, mark_price).await?;
            }

            let context = StrategyContext {
                symbol,
                best_bid_ask: snapshot.best_bid_ask.clone(),
                features: feature_snapshot,
                regime,
                inventory,
                soft_inventory_base: pair.soft_inventory_base,
                max_inventory_base: pair.max_inventory_base,
                runtime_state: self.state(),
                risk_mode: risk_context_health_mode(&health, self.state()),
            };

            let outcome = self.strategy_coordinator.evaluate(&context);
            let decisions = self.risk_manager.evaluate(outcome.intents, &risk_context).await?;
            for decision in decisions {
                self.storage.persist_risk_decision(&decision).await?;
                self.telemetry.emit_risk_decision(&decision).await?;
                if let Some(report) = self.execution.execute(decision.clone()).await? {
                    self.handle_execution_report(&report).await?;
                    placed_reports += 1;
                }
            }
        }

        Ok(placed_reports)
    }

    pub async fn run_until_shutdown(mut self) -> Result<()> {
        self.bootstrap().await?;
        let mut interval = tokio::time::interval(Duration::from_millis(LOOP_INTERVAL_MS));

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    self.transition(RuntimeState::Shutdown, "received ctrl-c").await?;
                    self.shutdown_streams();
                    break;
                }
                _ = interval.tick() => {
                    if self.state() == RuntimeState::Shutdown {
                        break;
                    }
                    self.run_cycle().await?;
                }
            }
        }

        Ok(())
    }

    async fn refresh_health(&mut self) {
        match self.gateway.ping_rest().await {
            Ok(_) => self.health_tracker.record_rest_success(now_utc()),
            Err(error) => {
                warn!(?error, "REST ping failed");
                self.health_tracker.record_rest_failure();
            }
        }

        match self.gateway.sync_clock().await {
            Ok(exchange_time) => {
                let drift_ms = self.health_tracker.record_clock_sample(exchange_time);
                info!(drift_ms, "clock drift sample updated");
            }
            Err(error) => {
                warn!(?error, "exchange clock sync failed");
                self.health_tracker.record_rest_failure();
            }
        }
    }

    async fn start_live_streams(&mut self) -> Result<()> {
        let mut fallback_active = false;

        match self.gateway.open_market_stream(&self.symbols).await {
            Ok(handle) => {
                self.market_stream = Some(handle);
                info!("market websocket stream initialized");
            }
            Err(error) => {
                fallback_active = true;
                warn!(?error, "failed to initialize market websocket stream");
            }
        }

        match self.gateway.open_user_stream().await {
            Ok(handle) => {
                self.user_stream = Some(handle);
                info!("user websocket stream initialized");
            }
            Err(error) => {
                fallback_active = true;
                warn!(?error, "failed to initialize user websocket stream");
            }
        }

        self.health_tracker.set_fallback_active(fallback_active);
        Ok(())
    }

    async fn ingest_stream_statuses(&mut self) -> Result<()> {
        let mut statuses = Vec::new();

        if let Some(handle) = self.market_stream.as_mut() {
            drain_receiver(&mut handle.status, &mut statuses);
        }

        if let Some(handle) = self.user_stream.as_mut() {
            drain_receiver(&mut handle.status, &mut statuses);
        }

        for status in statuses {
            self.health_tracker.apply_stream_status(&status);
            info!(
                kind = ?status.kind,
                lifecycle = ?status.lifecycle,
                reconnect_count = status.reconnect_count,
                detail = status.detail,
                "stream lifecycle update"
            );
        }

        Ok(())
    }

    async fn ingest_live_events(&mut self) -> Result<()> {
        let mut market_events = Vec::new();
        let mut user_events = Vec::new();

        if let Some(handle) = self.market_stream.as_mut() {
            drain_receiver(&mut handle.events, &mut market_events);
        }

        if let Some(handle) = self.user_stream.as_mut() {
            drain_receiver(&mut handle.events, &mut user_events);
        }

        for event in market_events {
            self.handle_market_event(event).await?;
        }

        for event in user_events {
            self.handle_user_event(event).await?;
        }

        Ok(())
    }

    async fn ingest_market_events(&mut self) -> Result<()> {
        for event in self.gateway.poll_market_events(&self.symbols).await? {
            self.handle_market_event(event).await?;
        }

        Ok(())
    }

    async fn ingest_account_events(&mut self) -> Result<()> {
        for event in self.gateway.poll_account_events(&self.symbols).await? {
            self.handle_user_event(event).await?;
        }

        Ok(())
    }

    async fn handle_market_event(&mut self, event: BinanceMarketEvent) -> Result<()> {
        match event {
            BinanceMarketEvent::OrderBookSnapshot(snapshot) => {
                self.health_tracker
                    .record_orderbook(snapshot.symbol, snapshot.observed_at);
                self.market_data.apply_orderbook_snapshot(snapshot).await?;
            }
            BinanceMarketEvent::OrderBookDelta(delta) => {
                self.health_tracker.record_orderbook(delta.symbol, delta.received_at);
                if let Err(error) = self.market_data.apply_orderbook_delta(delta.clone()).await {
                    warn!(symbol = %delta.symbol, ?error, "failed to apply order book delta, forcing resync");
                }
            }
            BinanceMarketEvent::Trade(trade) => {
                self.health_tracker.record_trade(trade.symbol, trade.event_time);
                self.market_data.apply_trade(trade).await?;
            }
        }

        Ok(())
    }

    async fn handle_user_event(&mut self, event: BinanceUserStreamEvent) -> Result<()> {
        match event {
            BinanceUserStreamEvent::AccountSnapshot(snapshot) => {
                self.health_tracker.record_account_snapshot(snapshot.updated_at);
                self.portfolio.apply_account_snapshot(snapshot).await?;
            }
            BinanceUserStreamEvent::AccountPosition { balances, updated_at } => {
                self.health_tracker.record_balance_update(updated_at);
                self.portfolio
                    .apply_account_position(balances, updated_at)
                    .await?;
            }
            BinanceUserStreamEvent::BalanceDelta {
                asset,
                delta,
                event_time,
                ..
            } => {
                self.health_tracker.record_balance_update(event_time);
                self.portfolio
                    .apply_balance_delta(asset, delta, event_time)
                    .await?;
            }
            BinanceUserStreamEvent::Execution(execution) => {
                self.handle_execution_event(execution).await?;
            }
            BinanceUserStreamEvent::EventStreamTerminated { event_time } => {
                self.health_tracker.apply_stream_status(&StreamStatus::disconnected(
                    StreamKind::UserWs,
                    0,
                    "binance user stream terminated",
                ));
                warn!(at = ?event_time, "user event stream terminated by exchange");
            }
        }

        Ok(())
    }

    async fn handle_execution_event(&mut self, execution: BinanceExecutionEvent) -> Result<()> {
        self.health_tracker.record_user_message(execution.transaction_time);
        self.execution.apply_execution_event(&execution).await?;
        self.handle_execution_report(&execution.report).await?;

        if let Some(fill) = execution.fill.clone() {
            if self.seen_fills.insert(fill.trade_id.clone()) {
                self.health_tracker.record_fill(fill.event_time);
                self.portfolio.apply_fill(fill.clone()).await?;
                self.accounting.record_fill(&fill).await?;
                self.storage.persist_fill(&fill).await?;
            }
        }

        Ok(())
    }

    async fn apply_bootstrap_state(&mut self, bootstrap: &BinanceBootstrapState) -> Result<()> {
        self.health_tracker.record_account_snapshot(bootstrap.account.updated_at);
        self.portfolio
            .apply_account_snapshot(bootstrap.account.clone())
            .await?;

        for fill in &bootstrap.fills {
            if self.seen_fills.insert(fill.trade_id.clone()) {
                self.health_tracker.record_fill(fill.event_time);
                self.portfolio.apply_fill(fill.clone()).await?;
                self.accounting.record_fill(fill).await?;
                self.storage.persist_fill(fill).await?;
            }
        }

        self.execution
            .sync_open_orders(bootstrap.open_orders.clone())
            .await?;
        Ok(())
    }

    async fn assert_bootstrap_coherent(
        &mut self,
        reconciled_orders: &[OpenOrder],
        drift_ms: i64,
    ) -> Result<()> {
        if drift_ms.abs() > self.config.risk.max_clock_drift_ms {
            self.transition(RuntimeState::RiskOff, "clock drift too high during bootstrap")
                .await?;
            bail!("bootstrap coherence failed: clock drift too high");
        }

        let Some(account) = self.portfolio.account_snapshot().await else {
            self.transition(RuntimeState::RiskOff, "missing account state after bootstrap")
                .await?;
            bail!("bootstrap coherence failed: missing account snapshot");
        };

        if account.balances.is_empty() {
            self.transition(RuntimeState::RiskOff, "empty account snapshot").await?;
            bail!("bootstrap coherence failed: empty account snapshot");
        }

        for &symbol in &self.symbols {
            if self.market_data.snapshot(symbol).await.is_none() {
                self.transition(RuntimeState::RiskOff, "missing initial market data")
                    .await?;
                bail!("bootstrap coherence failed: missing market snapshot for {symbol}");
            }
        }

        if self.config.runtime.bootstrap_cancel_open_orders && !reconciled_orders.is_empty() {
            self.transition(RuntimeState::RiskOff, "open orders remained after bootstrap cancel")
                .await?;
            bail!("bootstrap coherence failed: open orders remained after cancel-all");
        }

        Ok(())
    }

    async fn collect_inventory(&self) -> HashMap<Symbol, InventorySnapshot> {
        let mut inventory = HashMap::new();
        for &symbol in &self.symbols {
            inventory.insert(
                symbol,
                self.portfolio
                    .inventory(symbol)
                    .await
                    .unwrap_or_else(|| empty_inventory(symbol)),
            );
        }
        inventory
    }

    async fn collect_mid_prices(&self) -> HashMap<Symbol, Decimal> {
        let mut mid_prices = HashMap::new();
        for &symbol in &self.symbols {
            if let Some(MarketSnapshot {
                best_bid_ask: Some(best),
                ..
            }) = self.market_data.snapshot(symbol).await
            {
                mid_prices.insert(symbol, (best.bid_price + best.ask_price) / Decimal::from(2u32));
            }
        }
        mid_prices
    }

    async fn ensure_market_data_resynced(&mut self) -> Result<()> {
        let symbols = self.symbols.clone();
        for symbol in symbols {
            if self.market_data.needs_resync(symbol).await {
                warn!(symbol = %symbol, "market data marked stale, fetching fresh REST snapshot");
                let snapshot = self.gateway.fetch_orderbook_snapshot(symbol, 50).await?;
                self.market_data
                    .apply_orderbook_snapshot(snapshot.clone())
                    .await?;
                self.health_tracker
                    .record_orderbook(snapshot.symbol, snapshot.observed_at);
            }
        }
        Ok(())
    }

    async fn should_enable_fallback(&self) -> bool {
        if self.market_stream.is_none() || self.user_stream.is_none() {
            return true;
        }

        let health = self.health();
        health.market_ws.state != HealthState::Healthy
            || health.user_ws.state != HealthState::Healthy
            || health.market_data.state == HealthState::Stale
            || health.account_events.state == HealthState::Stale
    }

    async fn align_runtime_state_with_health(&mut self, health: &SystemHealth) -> Result<()> {
        if matches!(self.state(), RuntimeState::Trading | RuntimeState::Reduced)
            && health.overall_state != HealthState::Healthy
        {
            self.transition(RuntimeState::RiskOff, "health is not healthy")
                .await?;
        } else if self.state() == RuntimeState::Trading && health.fallback_active {
            self.transition(RuntimeState::Reduced, "REST fallback active").await?;
        } else if self.state() == RuntimeState::Reduced
            && !health.fallback_active
            && self.config.live.trading_enabled
        {
            self.transition(RuntimeState::Trading, "websocket streams recovered")
                .await?;
        }

        if self.state() == RuntimeState::RiskOff && self.config.live.cancel_all_on_risk_off {
            self.execution.cancel_all(None).await?;
        }

        Ok(())
    }

    async fn handle_execution_report(&mut self, report: &ExecutionReport) -> Result<()> {
        self.storage.persist_execution_report(report).await?;
        self.telemetry.emit_execution_report(report).await?;
        Ok(())
    }

    async fn transition(&mut self, next: RuntimeState, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        if let Some(transition) = self.state_machine.transition(next, reason.clone())? {
            self.storage.persist_runtime_transition(&transition).await?;
            self.telemetry
                .emit_runtime_state(&self.state_machine.snapshot())
                .await?;
            info!(from = ?transition.from, to = ?transition.to, reason = transition.reason, "runtime transition");
        }
        Ok(())
    }

    fn shutdown_streams(&mut self) {
        if let Some(handle) = self.market_stream.as_mut() {
            let _ = handle.shutdown.send(true);
        }

        if let Some(handle) = self.user_stream.as_mut() {
            let _ = handle.shutdown.send(true);
        }
    }
}

fn empty_inventory(symbol: Symbol) -> InventorySnapshot {
    InventorySnapshot {
        symbol,
        base_position: Decimal::ZERO,
        quote_position: Decimal::ZERO,
        mark_price: None,
        updated_at: now_utc(),
    }
}

fn risk_context_health_mode(health: &SystemHealth, runtime_state: RuntimeState) -> domain::RiskMode {
    if runtime_state == RuntimeState::Reduced {
        domain::RiskMode::Reduced
    } else if health.overall_state != HealthState::Healthy {
        domain::RiskMode::RiskOff
    } else if !runtime_state.allows_new_orders() {
        domain::RiskMode::Paused
    } else {
        domain::RiskMode::Normal
    }
}

fn drain_receiver<T>(receiver: &mut tokio::sync::mpsc::Receiver<T>, target: &mut Vec<T>) {
    loop {
        match receiver.try_recv() {
            Ok(item) => target.push(item),
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config_loader::{
        AppConfig, BinanceConfig, DashboardConfig, InventoryControlConfig, LiveConfig, PairConfig,
        PairsConfig, RiskConfig, RuntimeConfig,
    };
    use domain::{
        AccountSnapshot, Balance, ExecutionReport, FillEvent, OrderBookDelta, OrderBookSnapshot,
        OrderStatus, OrderType, PriceLevel, RuntimeState, Side, TimeInForce,
    };
    use exchange_binance_spot::{
        BinanceExecutionEvent, BinanceUserStreamEvent, MockBinanceSpotGateway, StreamKind,
        StreamStatus,
    };

    fn sample_config(trading_enabled: bool) -> AppConfig {
        AppConfig {
            runtime: RuntimeConfig {
                environment: "test".to_string(),
                state_dir: "var/state".to_string(),
                event_log_dir: "var/log".to_string(),
                bootstrap_cancel_open_orders: true,
            },
            binance: BinanceConfig {
                rest_base_url: "https://api.binance.com".to_string(),
                market_ws_url: "wss://stream.binance.com:9443/stream".to_string(),
                user_ws_url: "wss://stream.binance.com:9443/ws".to_string(),
                api_key_env: "BINANCE_API_KEY".to_string(),
                api_secret_env: "BINANCE_API_SECRET".to_string(),
                recv_window_ms: 5_000,
            },
            live: LiveConfig {
                trading_enabled,
                start_in_paused_mode: !trading_enabled,
                allow_market_orders: false,
                cancel_all_on_risk_off: true,
            },
            pairs: PairsConfig {
                pairs: vec![PairConfig {
                    symbol: Symbol::BtcUsdc,
                    enabled: true,
                    max_quote_notional: Decimal::from(500u32),
                    max_inventory_base: Decimal::from_str_exact("0.020").unwrap(),
                    soft_inventory_base: Decimal::from_str_exact("0.010").unwrap(),
                }],
            },
            risk: RiskConfig {
                max_daily_loss_usdc: Decimal::from(100u32),
                max_symbol_drawdown_usdc: Decimal::from(50u32),
                max_open_orders_per_symbol: 4,
                stale_market_data_ms: 5_000,
                stale_account_events_ms: 5_000,
                max_clock_drift_ms: 500,
                inventory: InventoryControlConfig {
                    quote_skew_bps_per_inventory_unit: Decimal::ONE,
                    neutralization_clip_fraction: Decimal::from_str_exact("0.20").unwrap(),
                    reduce_only_trigger_ratio: Decimal::from_str_exact("0.90").unwrap(),
                },
            },
            dashboard: DashboardConfig {
                bind_address: "127.0.0.1:8080".to_string(),
                enable_write_routes: false,
            },
        }
    }

    fn sample_account() -> AccountSnapshot {
        AccountSnapshot {
            balances: vec![
                Balance {
                    asset: "USDC".to_string(),
                    free: Decimal::from(1_000u32),
                    locked: Decimal::ZERO,
                },
                Balance {
                    asset: "BTC".to_string(),
                    free: Decimal::ZERO,
                    locked: Decimal::ZERO,
                },
            ],
            updated_at: now_utc(),
        }
    }

    fn sample_book() -> OrderBookSnapshot {
        OrderBookSnapshot {
            symbol: Symbol::BtcUsdc,
            bids: vec![PriceLevel {
                price: Decimal::from(60_000u32),
                quantity: Decimal::from_str_exact("0.5").unwrap(),
            }],
            asks: vec![PriceLevel {
                price: Decimal::from(60_010u32),
                quantity: Decimal::from_str_exact("0.4").unwrap(),
            }],
            last_update_id: 1,
            exchange_time: Some(now_utc()),
            observed_at: now_utc(),
        }
    }

    fn sample_trade() -> domain::MarketTrade {
        domain::MarketTrade {
            symbol: Symbol::BtcUsdc,
            trade_id: "trade-1".to_string(),
            price: Decimal::from(60_005u32),
            quantity: Decimal::from_str_exact("0.01").unwrap(),
            aggressor_side: Side::Buy,
            event_time: now_utc(),
        }
    }

    fn sample_execution_event(status: OrderStatus) -> BinanceExecutionEvent {
        BinanceExecutionEvent {
            report: ExecutionReport {
                client_order_id: "bot-123".to_string(),
                exchange_order_id: Some("42".to_string()),
                symbol: Symbol::BtcUsdc,
                status,
                filled_quantity: Decimal::from_str_exact("0.005").unwrap(),
                average_fill_price: Some(Decimal::from(60_000u32)),
                message: None,
                event_time: now_utc(),
            },
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::Gtc),
            original_quantity: Decimal::from_str_exact("0.010").unwrap(),
            price: Some(Decimal::from(60_000u32)),
            cumulative_filled_quantity: Decimal::from_str_exact("0.005").unwrap(),
            cumulative_quote_quantity: Decimal::from(300u32),
            last_executed_quantity: Decimal::from_str_exact("0.005").unwrap(),
            last_executed_price: Some(Decimal::from(60_000u32)),
            reject_reason: None,
            is_working: true,
            is_maker: false,
            order_created_at: Some(now_utc()),
            transaction_time: now_utc(),
            fill: Some(FillEvent {
                trade_id: "fill-1".to_string(),
                order_id: "42".to_string(),
                symbol: Symbol::BtcUsdc,
                side: Side::Buy,
                price: Decimal::from(60_000u32),
                quantity: Decimal::from_str_exact("0.005").unwrap(),
                fee: Decimal::from_str_exact("0.01").unwrap(),
                fee_asset: "USDC".to_string(),
                liquidity_side: domain::LiquiditySide::Taker,
                event_time: now_utc(),
            }),
        }
    }

    #[tokio::test]
    async fn runtime_bootstraps_and_enters_trading() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, vec![sample_trade()]).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone());
        runtime.bootstrap().await.unwrap();

        assert_eq!(runtime.state(), RuntimeState::Trading);
    }

    #[tokio::test]
    async fn pipeline_places_simple_orders_when_healthy() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, vec![sample_trade()]).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone());
        runtime.bootstrap().await.unwrap();
        let placed = runtime.run_cycle().await.unwrap();

        assert!(placed > 0);
        assert!(!gateway.placed_orders().await.is_empty());
    }

    #[tokio::test]
    async fn runtime_switches_to_reduced_when_stream_reconnects() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, vec![sample_trade()]).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone());
        runtime.bootstrap().await.unwrap();
        gateway
            .emit_market_stream_status(StreamStatus::reconnecting(
                StreamKind::MarketWs,
                1,
                "reconnect test",
            ))
            .await;

        runtime.run_cycle().await.unwrap();

        assert_eq!(runtime.state(), RuntimeState::Reduced);
        assert!(runtime.health().fallback_active);
        assert_eq!(runtime.health().market_ws.reconnect_count, 1);
    }

    #[tokio::test]
    async fn user_stream_execution_event_updates_execution_and_portfolio() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, vec![sample_trade()]).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(false), gateway.clone());
        runtime.bootstrap().await.unwrap();
        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(sample_execution_event(
                OrderStatus::PartiallyFilled,
            )))
            .await;

        runtime.run_cycle().await.unwrap();

        let inventory = runtime.portfolio.inventory(Symbol::BtcUsdc).await.unwrap();
        let open_orders = runtime.execution.open_orders().await;
        assert!(inventory.base_position > Decimal::ZERO);
        assert_eq!(open_orders.len(), 1);
        assert_eq!(runtime.state(), RuntimeState::Paused);
        assert_eq!(open_orders[0].status, OrderStatus::PartiallyFilled);
    }

    #[tokio::test]
    async fn gap_in_market_delta_triggers_resync_from_rest_snapshot() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, vec![sample_trade()]).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone());
        runtime.bootstrap().await.unwrap();
        gateway
            .emit_market_stream_event(BinanceMarketEvent::OrderBookDelta(OrderBookDelta {
                symbol: Symbol::BtcUsdc,
                first_update_id: 10,
                final_update_id: 11,
                bids: vec![PriceLevel {
                    price: Decimal::from(59_999u32),
                    quantity: Decimal::from_str_exact("0.25").unwrap(),
                }],
                asks: vec![PriceLevel {
                    price: Decimal::from(60_011u32),
                    quantity: Decimal::from_str_exact("0.25").unwrap(),
                }],
                exchange_time: now_utc(),
                received_at: now_utc(),
            }))
            .await;

        runtime.run_cycle().await.unwrap();

        assert!(!runtime.market_data.needs_resync(Symbol::BtcUsdc).await);
        assert!(runtime.market_data.snapshot(Symbol::BtcUsdc).await.is_some());
    }
}
