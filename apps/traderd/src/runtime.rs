use crate::{HealthTracker, RuntimeStateMachine};
use accounting::{AccountingService, InMemoryAccountingService};
use anyhow::{bail, Context, Result};
use common::{Decimal, now_utc};
use config_loader::AppConfig;
use domain::{
    ControlPlaneDecision, ControlPlaneRequest, ExecutionReport, ExitStage, HealthState,
    IntentRole, InventorySnapshot, MarketSnapshot, OpenOrder, OperatorCommand,
    PositionExitEvent, RuntimeState, StrategyContext, Symbol, SymbolBudget, SystemHealth,
};
use execution::{BinanceExecutionEngine, ExecutionEngine};
use exchange_binance_spot::{
    BinanceBootstrapState, BinanceExecutionEvent, BinanceFillRecoveryRequest, BinanceMarketEvent, BinanceSpotGateway,
    BinanceUserStreamEvent, MarketStreamHandle, StreamKind, StreamStatus, UserStreamHandle,
};
use features::{FeatureEngine, SimpleFeatureEngine};
use market_data::{InMemoryMarketDataService, MarketDataService};
use portfolio::{InMemoryPortfolioService, PortfolioService};
use regime::{ExecutionRegimeDetector, RegimeDetector};
use risk::{RiskContext, RiskManager, StrictRiskManager};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage::{MemoryStorage, SqliteStorage, StorageEngine};
use strategy_coordinator::{
    DefaultStrategyCoordinator, StrategySelection, StrategySelectionConfig,
};
use strategy_mm::{MarketMakingConfig, MarketMakingStrategy};
use strategy_scalp::{ScalpingConfig, ScalpingStrategy};
use telemetry::{TelemetrySink, TracingTelemetry};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{info, warn};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FillKey {
    symbol: Symbol,
    trade_id: String,
}

#[derive(Debug, Clone)]
struct ExitMetadata {
    intent_role: Option<IntentRole>,
    exit_stage: Option<ExitStage>,
    exit_reason: Option<String>,
}

impl FillKey {
    fn from_fill(fill: &domain::FillEvent) -> Self {
        Self {
            symbol: fill.symbol,
            trade_id: fill.trade_id.clone(),
        }
    }

    fn from_recovery(recovery: &BinanceFillRecoveryRequest) -> Self {
        Self {
            symbol: recovery.symbol,
            trade_id: recovery.trade_id.clone(),
        }
    }
}

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
    storage: Arc<dyn StorageEngine>,
    telemetry: TracingTelemetry,
    seen_fills: HashSet<FillKey>,
    pending_fill_recoveries: HashMap<FillKey, BinanceFillRecoveryRequest>,
    exit_metadata_by_exchange_order_id: HashMap<String, ExitMetadata>,
    operator_pause_active: bool,
    consecutive_cycle_failures: u32,
    risk_off_cancel_applied: bool,
    market_stream: Option<MarketStreamHandle>,
    user_stream: Option<UserStreamHandle>,
}

impl<G> TraderRuntime<G>
where
    G: BinanceSpotGateway + Clone + Send + Sync + 'static,
{
    pub fn new(config: AppConfig, gateway: G) -> Result<Self> {
        config.validate()?;
        let symbols = config.enabled_symbols();
        let risk_limits = config.risk_limits();
        let operator_pause_active =
            config.live.start_in_paused_mode || !config.live.trading_enabled;
        let budgets = config
            .pairs
            .pairs
            .iter()
            .filter(|pair| pair.enabled)
            .map(|pair| SymbolBudget {
                symbol: pair.symbol,
                max_quote_notional: pair.max_quote_notional,
                reserved_quote_notional: Decimal::ZERO,
                soft_inventory_base: pair.soft_inventory_base,
                max_inventory_base: pair.max_inventory_base,
                neutralization_clip_fraction: risk_limits.inventory.neutralization_clip_fraction,
                reduce_only_trigger_ratio: risk_limits.inventory.reduce_only_trigger_ratio,
            })
            .collect::<Vec<_>>();

        let storage_path = PathBuf::from(&config.runtime.state_dir).join("traderd.sqlite3");
        let storage: Arc<dyn StorageEngine> = if config.runtime.environment.eq_ignore_ascii_case("test") {
            Arc::new(MemoryStorage::default())
        } else {
            Arc::new(SqliteStorage::new(storage_path)?)
        };
        let strategy_coordinator = build_strategy_coordinator(&config);

        Ok(Self {
            risk_manager: StrictRiskManager::new(risk_limits),
            execution: BinanceExecutionEngine::new(gateway.clone(), symbols.clone()),
            portfolio: InMemoryPortfolioService::new(budgets),
            config,
            symbols,
            gateway,
            state_machine: RuntimeStateMachine::new(RuntimeState::Bootstrap, "runtime initialized"),
            health_tracker: HealthTracker::default(),
            market_data: InMemoryMarketDataService::default(),
            features: SimpleFeatureEngine::default(),
            regime_detector: ExecutionRegimeDetector::default(),
            strategy_coordinator,
            accounting: InMemoryAccountingService::default(),
            storage,
            telemetry: TracingTelemetry,
            seen_fills: HashSet::new(),
            pending_fill_recoveries: HashMap::new(),
            exit_metadata_by_exchange_order_id: HashMap::new(),
            operator_pause_active,
            consecutive_cycle_failures: 0,
            risk_off_cancel_applied: false,
            market_stream: None,
            user_stream: None,
        })
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

        let rest_started_at = now_utc();
        self.gateway.ping_rest().await?;
        let rest_observed_at = now_utc();
        self.health_tracker.record_rest_success(
            rest_observed_at,
            (rest_observed_at - rest_started_at).whole_milliseconds() as i64,
        );

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

            for trade in self
                .gateway
                .fetch_recent_trades(
                    symbol,
                    self.config.calibration.execution.bootstrap_trade_seed_limit,
                )
                .await?
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
            self.operator_pause_active = false;
            self.transition(RuntimeState::Trading, "live trading enabled").await?;
        } else {
            self.operator_pause_active = true;
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
        self.recover_pending_fills().await?;
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

        self.align_runtime_state_with_health(&health, fallback_active)
            .await?;

        let account = self
            .portfolio
            .account_snapshot()
            .await
            .context("missing account snapshot before pipeline evaluation")?;
        self.storage.persist_account_snapshot(&account).await?;
        let open_orders = self.execution.open_orders().await;
        let mid_prices = self.collect_mid_prices().await;
        for (&symbol, &mark_price) in &mid_prices {
            self.accounting
                .mark_to_market(symbol, mark_price, now_utc())
                .await?;
            self.portfolio.apply_mark_price(symbol, mark_price).await?;
        }
        let inventory_map = self.collect_inventory().await;
        let budgets = self.collect_budgets().await;
        let inventory_snapshots = inventory_map.values().cloned().collect::<Vec<_>>();
        self.storage.persist_health_snapshot(&health).await?;
        self.storage
            .persist_inventory_snapshots(&inventory_snapshots, &budgets)
            .await?;
        let pnl_snapshot = self.accounting.snapshot().await;
        self.storage.persist_pnl_snapshot(&pnl_snapshot).await?;
        self.telemetry.emit_pnl_snapshot(&pnl_snapshot).await?;
        let execution_stats = self.execution.stats().await;
        self.storage.persist_execution_stats(&execution_stats).await?;
        self.telemetry.emit_execution_stats(&execution_stats).await?;
        self.execution
            .cancel_stale_orders(
                self.config.calibration.execution.stale_order_cancel_ms,
                &mid_prices,
            )
            .await?;
        let risk_context = RiskContext {
            account: account.clone(),
            health: health.clone(),
            runtime_state: self.state(),
            inventory: inventory_map.clone(),
            open_orders: open_orders.clone(),
            mid_prices,
            pnl: pnl_snapshot.clone(),
            execution_stats: execution_stats.clone(),
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
            self.telemetry.emit_features(&feature_snapshot).await?;
            let regime = self
                .regime_detector
                .detect(symbol, &feature_snapshot, &health)
                .await?;
            self.telemetry.emit_regime(&regime).await?;
            let inventory = inventory_map
                .get(&symbol)
                .cloned()
                .unwrap_or_else(|| empty_inventory(symbol));
            let open_bot_orders_for_symbol = open_orders
                .iter()
                .filter(|order| {
                    order.symbol == symbol
                        && order.status.is_open()
                        && is_bot_managed_client_order_id(&order.client_order_id)
                })
                .count() as u32;

            let context = StrategyContext {
                symbol,
                best_bid_ask: snapshot.best_bid_ask.clone(),
                features: feature_snapshot,
                regime,
                inventory,
                soft_inventory_base: pair.soft_inventory_base,
                max_inventory_base: pair.max_inventory_base,
                local_min_notional_quote: self.config.calibration.execution.local_min_notional_quote,
                open_bot_orders_for_symbol,
                max_open_orders_for_symbol: self.config.risk.max_open_orders_per_symbol,
                runtime_state: self.state(),
                risk_mode: risk_context_health_mode(&health, self.state()),
            };

            let selected_strategy = self.strategy_coordinator.select(&context);
            let outcome = self.strategy_coordinator.evaluate(&context);
            self.telemetry
                .emit_strategy_outcome(
                    strategy_selection_label(selected_strategy),
                    &context,
                    &outcome,
                )
                .await?;
            self.storage
                .persist_strategy_outcome(
                    strategy_selection_label(selected_strategy),
                    &context,
                    &outcome,
                )
                .await?;
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

        self.consecutive_cycle_failures = 0;
        Ok(placed_reports)
    }

    pub async fn apply_control_plane_request(
        &mut self,
        request: ControlPlaneRequest,
    ) -> Result<ControlPlaneDecision> {
        let health = self.health();
        let mut decision = self
            .risk_manager
            .authorize_operator_command(&request.command, self.state(), &health)
            .await?;
        if !decision.accepted {
            info!(
                command = ?request.command,
                state = ?self.state(),
                message = decision.message.as_str(),
                "control-plane request rejected"
            );
            return Ok(decision);
        }

        info!(
            command = ?request.command,
            state = ?self.state(),
            submitted_by = request.submitted_by.as_str(),
            "control-plane request accepted"
        );

        match request.command {
            OperatorCommand::Pause => {
                self.operator_pause_active = true;
                self.transition(
                    RuntimeState::Paused,
                    format!("operator pause requested by {}", request.submitted_by),
                )
                .await?;
                decision.message = "operator pause applied by runtime".to_string();
            }
            OperatorCommand::Resume => {
                if !self.config.live.trading_enabled {
                    decision.accepted = false;
                    decision.message =
                        "operator resume rejected because live trading is disabled in config"
                            .to_string();
                    return Ok(decision);
                }

                if self.state() == RuntimeState::Trading {
                    decision.message = "operator resume ignored because runtime is already trading"
                        .to_string();
                    return Ok(decision);
                }

                self.operator_pause_active = false;
                let ws_degraded_reason =
                    self.websocket_degradation_reason(&health, self.should_enable_fallback().await);
                let recovery_reason = self.recovery_reason(&health).await;
                if ws_degraded_reason.is_some() || recovery_reason.is_some() {
                    let gating_reason = recovery_reason.unwrap_or_else(|| {
                        ws_degraded_reason.unwrap_or_else(|| {
                            "operator resume requested but runtime health is not ready".to_string()
                        })
                    });
                    if self.state() != RuntimeState::Reconciling {
                        self.transition(
                            RuntimeState::Reconciling,
                            format!(
                                "operator resume requested by {}; {}",
                                request.submitted_by, gating_reason
                            ),
                        )
                        .await?;
                    }
                    decision.message =
                        "operator resume accepted; runtime moved into reconciliation".to_string();
                } else if self.state() == RuntimeState::Reduced {
                    self.transition(
                        RuntimeState::Trading,
                        "operator resume conditions satisfied",
                    )
                    .await?;
                    decision.message = "operator resume applied by runtime".to_string();
                } else {
                    if self.state() == RuntimeState::RiskOff {
                        self.transition(
                            RuntimeState::Reconciling,
                            format!(
                                "operator resume requested by {}; exiting risk-off through reconciliation",
                                request.submitted_by
                            ),
                        )
                        .await?;
                    }
                    if self.state() != RuntimeState::Ready {
                        self.transition(
                            RuntimeState::Ready,
                            format!("operator resume requested by {}", request.submitted_by),
                        )
                        .await?;
                    }
                    if self.state() != RuntimeState::Trading {
                        self.transition(
                            RuntimeState::Trading,
                            "operator resume conditions satisfied",
                        )
                        .await?;
                    }
                    decision.message = "operator resume applied by runtime".to_string();
                }
            }
            OperatorCommand::EnterReduced => {
                self.transition(
                    RuntimeState::Reduced,
                    format!("operator reduced mode requested by {}", request.submitted_by),
                )
                .await?;
                decision.message = "operator reduced mode applied by runtime".to_string();
            }
            OperatorCommand::EnterRiskOff => {
                self.transition(
                    RuntimeState::RiskOff,
                    format!("operator risk-off requested by {}", request.submitted_by),
                )
                .await?;
                decision.message = "operator risk-off applied by runtime".to_string();
            }
            OperatorCommand::CancelAll { symbol } => {
                self.execution.cancel_all(symbol).await?;
                decision.message = "operator cancel-all applied by runtime".to_string();
            }
            OperatorCommand::Flatten { symbol } => {
                self.execution.cancel_all(Some(symbol)).await?;
                decision.message =
                    "operator flatten request canceled open orders for the symbol; active flatten execution is not wired"
                        .to_string();
            }
            OperatorCommand::ReloadConfig => {
                decision.message =
                    "operator reload-config accepted but runtime hot reload is not wired".to_string();
            }
            OperatorCommand::Shutdown => {
                self.transition(
                    RuntimeState::Shutdown,
                    format!("operator shutdown requested by {}", request.submitted_by),
                )
                .await?;
                decision.message = "operator shutdown applied by runtime".to_string();
            }
        }

        decision.resulting_state = self.state();
        Ok(decision)
    }

    pub async fn run_until_shutdown(mut self) -> Result<()> {
        self.bootstrap().await?;
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.config.calibration.execution.loop_interval_ms,
        ));

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    self.transition(RuntimeState::Shutdown, "received ctrl-c").await?;
                    break;
                }
                _ = interval.tick() => {
                    if self.state() == RuntimeState::Shutdown {
                        break;
                    }
                    match self.run_cycle().await {
                        Ok(_) => {}
                        Err(error) => {
                            self.handle_cycle_error(&error).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn refresh_health(&mut self) {
        let rest_started_at = now_utc();
        match self.gateway.ping_rest().await {
            Ok(_) => {
                let rest_observed_at = now_utc();
                self.health_tracker.record_rest_success(
                    rest_observed_at,
                    (rest_observed_at - rest_started_at).whole_milliseconds() as i64,
                )
            }
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
            if matches!(status.kind, StreamKind::MarketWs) {
                self.market_data
                    .set_market_ws_reconnect_counter(status.reconnect_count)
                    .await?;
            }
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
            self.handle_market_event(event, true).await?;
        }

        for event in user_events {
            self.handle_user_event(event, true).await?;
        }

        Ok(())
    }

    async fn ingest_market_events(&mut self) -> Result<()> {
        for event in self.gateway.poll_market_events(&self.symbols).await? {
            self.handle_market_event(event, false).await?;
        }

        Ok(())
    }

    async fn ingest_account_events(&mut self) -> Result<()> {
        for event in self.gateway.poll_account_events(&self.symbols).await? {
            self.handle_user_event(event, false).await?;
        }

        Ok(())
    }

    async fn handle_market_event(&mut self, event: BinanceMarketEvent, from_ws: bool) -> Result<()> {
        match event {
            BinanceMarketEvent::OrderBookSnapshot(snapshot) => {
                if from_ws {
                    self.health_tracker
                        .record_market_ws_message(snapshot.observed_at);
                }
                self.health_tracker
                    .record_orderbook(snapshot.symbol, snapshot.observed_at);
                self.market_data.apply_orderbook_snapshot(snapshot).await?;
            }
            BinanceMarketEvent::OrderBookDelta(delta) => {
                if from_ws {
                    self.health_tracker
                        .record_market_ws_message(delta.received_at);
                }
                self.health_tracker.record_orderbook(delta.symbol, delta.received_at);
                if let Err(error) = self.market_data.apply_orderbook_delta(delta.clone()).await {
                    warn!(symbol = %delta.symbol, ?error, "failed to apply order book delta, forcing resync");
                }
            }
            BinanceMarketEvent::Trade(trade) => {
                if from_ws {
                    self.health_tracker
                        .record_market_ws_message(trade.received_at);
                }
                self.health_tracker.record_trade(trade.symbol, trade.event_time);
                self.market_data.apply_trade(trade).await?;
            }
        }

        Ok(())
    }

    async fn handle_user_event(&mut self, event: BinanceUserStreamEvent, from_ws: bool) -> Result<()> {
        match event {
            BinanceUserStreamEvent::AccountSnapshot(snapshot) => {
                if from_ws {
                    self.health_tracker
                        .record_user_ws_message(snapshot.updated_at);
                }
                self.health_tracker.record_account_snapshot(snapshot.updated_at);
                self.portfolio.apply_account_snapshot(snapshot).await?;
            }
            BinanceUserStreamEvent::AccountPosition { balances, updated_at } => {
                if from_ws {
                    self.health_tracker.record_user_ws_message(updated_at);
                }
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
                if from_ws {
                    self.health_tracker.record_user_ws_message(event_time);
                }
                self.health_tracker.record_balance_update(event_time);
                self.portfolio
                    .apply_balance_delta(asset, delta, event_time)
                    .await?;
            }
            BinanceUserStreamEvent::Execution(execution) => {
                self.handle_execution_event(execution, from_ws).await?;
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

    async fn handle_execution_event(&mut self, execution: BinanceExecutionEvent, from_ws: bool) -> Result<()> {
        if from_ws {
            self.health_tracker
                .record_user_ws_message(execution.transaction_time);
        }
        let report = self.execution.apply_execution_event(&execution).await?;
        self.handle_execution_report(&report).await?;

        if let Some(fill) = execution.fill.clone() {
            self.apply_fill_if_new(fill).await?;
        }

        if let Some(recovery) = execution.fill_recovery.clone() {
            warn!(
                symbol = %recovery.symbol,
                trade_id = %recovery.trade_id,
                order_id = %recovery.order_id,
                fee_asset = %recovery.fee_asset,
                reason = %recovery.reason,
                "queued fill for REST recovery"
            );
            self.pending_fill_recoveries
                .insert(FillKey::from_recovery(&recovery), recovery);
        }

        Ok(())
    }

    async fn apply_fill_if_new(&mut self, fill: domain::FillEvent) -> Result<bool> {
        let fill_key = FillKey::from_fill(&fill);
        if !self.seen_fills.insert(fill_key.clone()) {
            self.pending_fill_recoveries.remove(&fill_key);
            return Ok(false);
        }

        self.pending_fill_recoveries.remove(&fill_key);
        self.health_tracker.record_fill(fill.event_time);
        let before_inventory = self
            .portfolio
            .inventory(fill.symbol)
            .await
            .unwrap_or_else(|| empty_inventory(fill.symbol));
        self.portfolio.apply_fill(fill.clone()).await?;
        self.accounting.record_fill(&fill).await?;
        self.storage.persist_fill(&fill).await?;
        let after_inventory = self
            .portfolio
            .inventory(fill.symbol)
            .await
            .unwrap_or_else(|| empty_inventory(fill.symbol));
        self.maybe_persist_position_exit_event(&before_inventory, &after_inventory, &fill)
            .await?;
        Ok(true)
    }

    async fn recover_pending_fills(&mut self) -> Result<()> {
        if self.pending_fill_recoveries.is_empty() {
            return Ok(());
        }

        let mut recoveries_by_symbol: HashMap<Symbol, Vec<BinanceFillRecoveryRequest>> = HashMap::new();
        for recovery in self.pending_fill_recoveries.values().cloned() {
            recoveries_by_symbol
                .entry(recovery.symbol)
                .or_default()
                .push(recovery);
        }

        for (symbol, recoveries) in recoveries_by_symbol {
            let rest_started_at = now_utc();
            match self
                .gateway
                .fetch_recent_fills(
                    symbol,
                    self.config.calibration.execution.fill_recovery_fetch_limit,
                )
                .await
            {
                Ok(fills) => {
                    let rest_observed_at = now_utc();
                    self.health_tracker.record_rest_success(
                        rest_observed_at,
                        (rest_observed_at - rest_started_at).whole_milliseconds() as i64,
                    );
                    let recovered_by_trade = fills
                        .into_iter()
                        .map(|fill| (FillKey::from_fill(&fill), fill))
                        .collect::<HashMap<_, _>>();

                    for recovery in recoveries {
                        let fill_key = FillKey::from_recovery(&recovery);
                        if let Some(fill) = recovered_by_trade.get(&fill_key).cloned() {
                            self.apply_fill_if_new(fill).await?;
                            self.pending_fill_recoveries.remove(&fill_key);
                            info!(
                                symbol = %recovery.symbol,
                                trade_id = %recovery.trade_id,
                                order_id = %recovery.order_id,
                                "recovered fill via REST reconciliation"
                            );
                        } else {
                            warn!(
                                symbol = %recovery.symbol,
                                trade_id = %recovery.trade_id,
                                order_id = %recovery.order_id,
                                "pending fill recovery still waiting on REST reconciliation"
                            );
                        }
                    }
                }
                Err(error) => {
                    self.health_tracker.record_rest_failure();
                    warn!(
                        ?error,
                        symbol = %symbol,
                        pending = recoveries.len(),
                        "failed to recover pending fills via REST"
                    );
                }
            }
        }

        Ok(())
    }

    async fn apply_bootstrap_state(&mut self, bootstrap: &BinanceBootstrapState) -> Result<()> {
        for fill in &bootstrap.fills {
            self.apply_fill_if_new(fill.clone()).await?;
        }

        self.health_tracker.record_account_snapshot(bootstrap.account.updated_at);
        self.portfolio
            .apply_account_snapshot(bootstrap.account.clone())
            .await?;

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

    async fn collect_budgets(&self) -> Vec<SymbolBudget> {
        let mut budgets = Vec::new();
        for &symbol in &self.symbols {
            if let Some(budget) = self.portfolio.budget(symbol).await {
                budgets.push(budget);
            }
        }
        budgets
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

    async fn align_runtime_state_with_health(
        &mut self,
        health: &SystemHealth,
        fallback_active: bool,
    ) -> Result<()> {
        let hard_block_reason = self.hard_block_reason(health);
        let ws_degraded_reason = self.websocket_degradation_reason(health, fallback_active);
        let recovery_reason = self.recovery_reason(health).await;
        let recovery_required = recovery_reason.is_some();
        let ws_degraded = ws_degraded_reason.is_some();

        if matches!(self.state(), RuntimeState::Bootstrap | RuntimeState::Shutdown) {
            return Ok(());
        }

        if let Some(reason) = hard_block_reason {
            self.transition(RuntimeState::RiskOff, reason)
                .await?;
            return Ok(());
        }

        if self.state() == RuntimeState::RiskOff {
            return Ok(());
        }

        if recovery_required
            && matches!(
                self.state(),
                RuntimeState::Trading | RuntimeState::Reduced | RuntimeState::Ready
            )
        {
            if let Some(reason) = recovery_reason {
                self.transition(RuntimeState::Reconciling, reason).await?;
            }
            return Ok(());
        }

        if matches!(self.state(), RuntimeState::Trading) && ws_degraded {
            if let Some(reason) = ws_degraded_reason {
                self.transition(RuntimeState::Reduced, reason).await?;
            }
            return Ok(());
        }

        if self.state() == RuntimeState::Reduced
            && self
                .websocket_degradation_reason(health, fallback_active)
                .is_none()
            && self.recovery_reason(health).await.is_none()
        {
            self.promote_runtime_after_recovery("live health recovered")
                .await?;
            return Ok(());
        }

        if self.state() == RuntimeState::Reconciling
            && self
                .websocket_degradation_reason(health, fallback_active)
                .is_none()
            && self.recovery_reason(health).await.is_none()
        {
            self.promote_runtime_after_recovery("runtime recovery completed")
                .await?;
            return Ok(());
        }

        if self.state() == RuntimeState::Ready
            && self
                .websocket_degradation_reason(health, fallback_active)
                .is_none()
            && self.recovery_reason(health).await.is_none()
        {
            self.promote_runtime_after_recovery("runtime ready for controlled resume")
                .await?;
        }

        Ok(())
    }

    async fn recovery_reason(&self, health: &SystemHealth) -> Option<String> {
        let mut reasons = Vec::new();
        if health.market_data.state == HealthState::Degraded {
            reasons.push("market_data=Degraded".to_string());
        }
        if health.clock_drift.state == HealthState::Degraded {
            reasons.push("clock_drift=Degraded".to_string());
        }
        if health.account_events.state != HealthState::Healthy {
            reasons.push(format!("account_events={:?}", health.account_events.state));
        }
        if !self.pending_fill_recoveries.is_empty() {
            reasons.push(format!(
                "pending_fill_recoveries={}",
                self.pending_fill_recoveries.len()
            ));
        }
        if self.has_market_data_resync_backlog().await {
            reasons.push("market_data_resync_backlog=true".to_string());
        }

        if reasons.is_empty() {
            None
        } else {
            Some(format!("runtime recovery gate active: {}", reasons.join(", ")))
        }
    }

    fn websocket_degradation_reason(
        &self,
        health: &SystemHealth,
        fallback_active: bool,
    ) -> Option<String> {
        let mut reasons = Vec::new();
        if health.market_ws.state != HealthState::Healthy {
            reasons.push(format!("market_ws={:?}", health.market_ws.state));
        }
        if health.user_ws.state != HealthState::Healthy {
            reasons.push(format!("user_ws={:?}", health.user_ws.state));
        }
        if fallback_active {
            reasons.push("rest_fallback_active=true".to_string());
        }

        if reasons.is_empty() {
            None
        } else {
            Some(format!(
                "websocket degradation detected: {}",
                reasons.join(", ")
            ))
        }
    }

    fn hard_block_reason(&self, health: &SystemHealth) -> Option<String> {
        let mut reasons = Vec::new();
        if health.market_data.state == HealthState::Stale {
            reasons.push("market_data=Stale".to_string());
        }
        if health.clock_drift.state == HealthState::Unhealthy {
            reasons.push(format!("clock_drift_ms={}", health.clock_drift.drift_ms));
        }
        if health.rest.state == HealthState::Unhealthy {
            reasons.push(format!(
                "rest_consecutive_failures={}",
                health.rest.consecutive_failures
            ));
        }

        if reasons.is_empty() {
            None
        } else {
            Some(format!("hard health block triggered: {}", reasons.join(", ")))
        }
    }

    async fn has_market_data_resync_backlog(&self) -> bool {
        for &symbol in &self.symbols {
            if self.market_data.needs_resync(symbol).await {
                return true;
            }
        }
        false
    }

    fn should_auto_resume_trading(&self) -> bool {
        self.config.live.trading_enabled && !self.operator_pause_active
    }

    async fn promote_runtime_after_recovery(&mut self, reason: &str) -> Result<()> {
        if matches!(self.state(), RuntimeState::Reconciling | RuntimeState::Paused | RuntimeState::RiskOff) {
            self.transition(RuntimeState::Ready, reason).await?;
        }
        if self.should_auto_resume_trading() {
            self.transition(
                RuntimeState::Trading,
                "runtime recovered and live trading resumed",
            )
            .await?;
        } else {
            self.transition(
                RuntimeState::Paused,
                "runtime recovered; waiting for operator resume",
            )
            .await?;
        }
        Ok(())
    }

    async fn handle_execution_report(&mut self, report: &ExecutionReport) -> Result<()> {
        if let Some(exchange_order_id) = &report.exchange_order_id {
            self.exit_metadata_by_exchange_order_id.insert(
                exchange_order_id.clone(),
                ExitMetadata {
                    intent_role: report.intent_role,
                    exit_stage: report.exit_stage,
                    exit_reason: report.exit_reason.clone().or_else(|| report.message.clone()),
                },
            );
        }
        self.storage.persist_execution_report(report).await?;
        self.telemetry.emit_execution_report(report).await?;
        Ok(())
    }

    async fn maybe_persist_position_exit_event(
        &mut self,
        before: &InventorySnapshot,
        after: &InventorySnapshot,
        fill: &domain::FillEvent,
    ) -> Result<()> {
        if before.base_position <= Decimal::ZERO || !after.base_position.is_zero() {
            return Ok(());
        }

        let Some(opened_at) = before.position_opened_at else {
            return Ok(());
        };
        let first_exit_at = before.first_reduce_at.or(Some(fill.event_time));
        let metadata = self.exit_metadata_by_exchange_order_id.get(&fill.order_id).cloned();
        let intent_role = metadata.as_ref().and_then(|value| value.intent_role);
        let exit_stage = metadata.as_ref().and_then(|value| value.exit_stage);
        let exit_reason = metadata
            .as_ref()
            .and_then(|value| value.exit_reason.clone())
            .or_else(|| Some("unclassified_exit".to_string()));
        let event = PositionExitEvent {
            symbol: fill.symbol,
            opened_at,
            first_exit_at,
            closed_at: fill.event_time,
            hold_time_ms: (fill.event_time - opened_at).whole_milliseconds() as i64,
            time_to_first_exit_ms: first_exit_at
                .map(|first_exit_at| (first_exit_at - opened_at).whole_milliseconds() as i64),
            intent_role,
            exit_stage,
            exit_reason,
            passive_exit: matches!(exit_stage, Some(ExitStage::Passive | ExitStage::Tighten)),
            aggressive_exit: matches!(exit_stage, Some(ExitStage::Aggressive | ExitStage::Emergency)),
            forced_unwind: matches!(intent_role, Some(IntentRole::ForcedUnwind | IntentRole::EmergencyExit)),
        };
        self.storage.persist_position_exit_event(&event).await?;
        self.exit_metadata_by_exchange_order_id.remove(&fill.order_id);
        Ok(())
    }

    async fn handle_cycle_error(&mut self, error: &anyhow::Error) -> Result<()> {
        self.consecutive_cycle_failures = self.consecutive_cycle_failures.saturating_add(1);
        warn!(
            ?error,
            failures = self.consecutive_cycle_failures,
            state = ?self.state(),
            "runtime cycle failed"
        );

        if self.consecutive_cycle_failures >= 3 {
            self.transition(
                RuntimeState::RiskOff,
                format!(
                    "repeated runtime cycle failures ({})",
                    self.consecutive_cycle_failures
                ),
            )
            .await?;
            return Ok(());
        }

        if matches!(
            self.state(),
            RuntimeState::Trading | RuntimeState::Reduced | RuntimeState::Ready
        ) {
            self.transition(
                RuntimeState::Reconciling,
                format!("runtime cycle error absorbed: {error}"),
            )
            .await?;
        }

        Ok(())
    }

    async fn transition(&mut self, next: RuntimeState, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        if let Some(transition) = self.state_machine.transition(next, reason.clone())? {
            if transition.to != RuntimeState::RiskOff {
                self.risk_off_cancel_applied = false;
            }
            self.storage.persist_runtime_transition(&transition).await?;
            self.telemetry
                .emit_runtime_state(&self.state_machine.snapshot())
                .await?;
            info!(from = ?transition.from, to = ?transition.to, reason = transition.reason, "runtime transition");
            if transition.to == RuntimeState::RiskOff
                && self.config.live.cancel_all_on_risk_off
                && !self.risk_off_cancel_applied
            {
                self.execution.cancel_all(None).await?;
                self.risk_off_cancel_applied = true;
            }
            if transition.to == RuntimeState::Shutdown {
                self.shutdown_streams();
            }
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
        average_entry_price: None,
        position_opened_at: None,
        last_fill_at: None,
        first_reduce_at: None,
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

fn strategy_selection_label(selection: StrategySelection) -> &'static str {
    match selection {
        StrategySelection::MarketMaking => "market_making",
        StrategySelection::Scalping => "scalping",
        StrategySelection::Standby => "standby",
        StrategySelection::RiskOff => "risk_off",
    }
}

fn is_bot_managed_client_order_id(client_order_id: &str) -> bool {
    client_order_id.starts_with("bot-")
}

fn build_strategy_coordinator(config: &AppConfig) -> DefaultStrategyCoordinator {
    DefaultStrategyCoordinator::new(
        StrategySelectionConfig {
            scalp_activation_momentum_bps: config
                .calibration
                .selection
                .scalp_activation_momentum_bps,
            scalp_activation_flow_imbalance: config
                .calibration
                .selection
                .scalp_activation_flow_imbalance,
        },
        MarketMakingStrategy {
            config: MarketMakingConfig {
                maker_fee_bps: config.calibration.market_making.maker_fee_bps,
                slippage_buffer_bps: config.calibration.market_making.slippage_buffer_bps,
                min_net_edge_bps: config.calibration.market_making.min_net_edge_bps,
                min_market_spread_bps: config.calibration.market_making.min_market_spread_bps,
                max_toxicity_score: config.calibration.market_making.max_toxicity_score,
                base_skew_bps: config.calibration.market_making.base_skew_bps,
                momentum_skew_weight: config.calibration.market_making.momentum_skew_weight,
                volatility_widening_weight: config
                    .calibration
                    .market_making
                    .volatility_widening_weight,
                quote_size_fraction: config.calibration.market_making.quote_size_fraction,
                quote_ttl_secs: config.calibration.market_making.quote_ttl_secs,
                soft_hold_secs: config.calibration.market_making.soft_hold_secs,
                stale_hold_secs: config.calibration.market_making.stale_hold_secs,
                aggressive_exit_secs: config.calibration.market_making.aggressive_exit_secs,
            },
        },
        ScalpingStrategy {
            config: ScalpingConfig {
                taker_fee_bps: config.calibration.scalping.taker_fee_bps,
                slippage_buffer_bps: config.calibration.scalping.slippage_buffer_bps,
                min_entry_score: config.calibration.scalping.min_entry_score,
                min_momentum_bps: config.calibration.scalping.min_momentum_bps,
                max_toxicity_score: config.calibration.scalping.max_toxicity_score,
                max_position_fraction: config.calibration.scalping.max_position_fraction,
                quote_size_fraction: config.calibration.scalping.quote_size_fraction,
                quote_ttl_secs: config.calibration.scalping.quote_ttl_secs,
                soft_hold_secs: config.calibration.scalping.soft_hold_secs,
                stale_hold_secs: config.calibration.scalping.stale_hold_secs,
                aggressive_exit_secs: config.calibration.scalping.aggressive_exit_secs,
            },
        },
    )
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
        AppConfig, BinanceConfig, CalibrationConfig, DashboardConfig, InventoryControlConfig,
        LiveConfig, PairConfig, PairsConfig, RiskConfig, RuntimeConfig,
    };
    use domain::{
        AccountSnapshot, Balance, ControlPlaneRequest, ExecutionReport, FillEvent, HealthState,
        OperatorCommand, OrderBookDelta, OrderBookSnapshot, OrderStatus, OrderType, PriceLevel,
        RuntimeState, Side, TimeInForce,
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
                pairs: vec![sample_pair(Symbol::BtcUsdc)],
            },
            risk: RiskConfig {
                max_daily_loss_usdc: Decimal::from(100u32),
                max_symbol_drawdown_usdc: Decimal::from(50u32),
                max_open_orders_per_symbol: 4,
                max_reject_rate: Decimal::from_str_exact("0.20").unwrap(),
                stale_market_data_ms: 5_000,
                stale_account_events_ms: 5_000,
                max_clock_drift_ms: 500,
                inventory: InventoryControlConfig {
                    quote_skew_bps_per_inventory_unit: Decimal::ONE,
                    neutralization_clip_fraction: Decimal::from_str_exact("0.20").unwrap(),
                    reduce_only_trigger_ratio: Decimal::from_str_exact("0.90").unwrap(),
                },
            },
            calibration: CalibrationConfig::default(),
            dashboard: DashboardConfig {
                bind_address: "127.0.0.1:8080".to_string(),
                enable_write_routes: false,
            },
        }
    }

    fn sample_pair(symbol: Symbol) -> PairConfig {
        PairConfig {
            symbol,
            enabled: true,
            max_quote_notional: Decimal::from(500u32),
            max_inventory_base: match symbol {
                Symbol::BtcUsdc => Decimal::from_str_exact("0.020").unwrap(),
                Symbol::EthUsdc => Decimal::from_str_exact("0.50").unwrap(),
            },
            soft_inventory_base: match symbol {
                Symbol::BtcUsdc => Decimal::from_str_exact("0.010").unwrap(),
                Symbol::EthUsdc => Decimal::from_str_exact("0.25").unwrap(),
            },
        }
    }

    fn sample_config_with_pairs(trading_enabled: bool, pairs: Vec<PairConfig>) -> AppConfig {
        let mut config = sample_config(trading_enabled);
        config.pairs = PairsConfig { pairs };
        config
    }

    fn sample_account() -> AccountSnapshot {
        sample_account_with_positions(Decimal::ZERO, Decimal::ZERO)
    }

    fn sample_account_with_positions(btc_free: Decimal, eth_free: Decimal) -> AccountSnapshot {
        AccountSnapshot {
            balances: vec![
                Balance {
                    asset: "USDC".to_string(),
                    free: Decimal::from(1_000u32),
                    locked: Decimal::ZERO,
                },
                Balance {
                    asset: "BTC".to_string(),
                    free: btc_free,
                    locked: Decimal::ZERO,
                },
                Balance {
                    asset: "ETH".to_string(),
                    free: eth_free,
                    locked: Decimal::ZERO,
                },
            ],
            updated_at: now_utc(),
        }
    }

    async fn mark_streams_healthy(gateway: &MockBinanceSpotGateway) {
        gateway
            .emit_market_stream_status(StreamStatus::connected(
                StreamKind::MarketWs,
                0,
                "market stream healthy",
            ))
            .await;
        gateway
            .emit_user_stream_status(StreamStatus::connected(
                StreamKind::UserWs,
                0,
                "user stream healthy",
            ))
            .await;
    }

    fn sample_book() -> OrderBookSnapshot {
        sample_book_for(Symbol::BtcUsdc)
    }

    fn sample_book_for(symbol: Symbol) -> OrderBookSnapshot {
        OrderBookSnapshot {
            symbol,
            bids: vec![PriceLevel {
                price: match symbol {
                    Symbol::BtcUsdc => Decimal::from(60_000u32),
                    Symbol::EthUsdc => Decimal::from(2_000u32),
                },
                quantity: Decimal::from_str_exact("0.5").unwrap(),
            }],
            asks: vec![PriceLevel {
                price: match symbol {
                    Symbol::BtcUsdc => Decimal::from(60_030u32),
                    Symbol::EthUsdc => Decimal::from(2_003u32),
                },
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
            price: Decimal::from(60_015u32),
            quantity: Decimal::from_str_exact("0.01").unwrap(),
            aggressor_side: Side::Buy,
            event_time: now_utc(),
            received_at: now_utc(),
        }
    }

    fn sample_trades() -> Vec<domain::MarketTrade> {
        let now = now_utc();
        vec![
            domain::MarketTrade {
                trade_id: "trade-1".to_string(),
                event_time: now - time::Duration::seconds(6),
                received_at: now - time::Duration::seconds(6),
                ..sample_trade()
            },
            domain::MarketTrade {
                trade_id: "trade-2".to_string(),
                price: Decimal::from(60_020u32),
                event_time: now - time::Duration::seconds(4),
                received_at: now - time::Duration::seconds(4),
                ..sample_trade()
            },
            domain::MarketTrade {
                trade_id: "trade-3".to_string(),
                price: Decimal::from(60_025u32),
                event_time: now - time::Duration::seconds(2),
                received_at: now - time::Duration::seconds(2),
                ..sample_trade()
            },
            domain::MarketTrade {
                trade_id: "trade-4".to_string(),
                price: Decimal::from(60_018u32),
                event_time: now,
                received_at: now,
                ..sample_trade()
            },
        ]
    }

    fn sample_execution_event(status: OrderStatus) -> BinanceExecutionEvent {
        sample_execution_event_for(Symbol::BtcUsdc, "fill-1", status)
    }

    fn sample_execution_event_for(
        symbol: Symbol,
        trade_id: &str,
        status: OrderStatus,
    ) -> BinanceExecutionEvent {
        let (price, quantity, cumulative_quote, fee_quote) = match symbol {
            Symbol::BtcUsdc => (
                Decimal::from(60_000u32),
                Decimal::from_str_exact("0.005").unwrap(),
                Decimal::from(300u32),
                Decimal::from_str_exact("0.01").unwrap(),
            ),
            Symbol::EthUsdc => (
                Decimal::from(2_000u32),
                Decimal::from_str_exact("0.050").unwrap(),
                Decimal::from(100u32),
                Decimal::from_str_exact("0.01").unwrap(),
            ),
        };
        BinanceExecutionEvent {
            report: ExecutionReport {
                client_order_id: "bot-123".to_string(),
                exchange_order_id: Some("42".to_string()),
                symbol,
                status,
                filled_quantity: quantity,
                average_fill_price: Some(price),
                fill_ratio: Decimal::from_str_exact("0.50").unwrap(),
                requested_price: Some(price),
                slippage_bps: Some(Decimal::ZERO),
                decision_latency_ms: Some(10),
                submit_ack_latency_ms: Some(25),
                submit_to_first_report_ms: Some(40),
                submit_to_fill_ms: Some(120),
                exchange_order_age_ms: Some(120),
                edge_after_cost_bps: Some(Decimal::from_str_exact("1.25").unwrap()),
                expected_realized_edge_bps: Some(Decimal::from_str_exact("0.78").unwrap()),
                adverse_selection_penalty_bps: Some(Decimal::from_str_exact("0.22").unwrap()),
                intent_role: Some(IntentRole::AddRisk),
                exit_stage: None,
                exit_reason: None,
                message: None,
                event_time: now_utc(),
            },
            side: Side::Buy,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::Gtc),
            original_quantity: quantity * Decimal::from(2u32),
            price: Some(price),
            cumulative_filled_quantity: quantity,
            cumulative_quote_quantity: cumulative_quote,
            last_executed_quantity: quantity,
            last_executed_price: Some(price),
            reject_reason: None,
            is_working: true,
            is_maker: false,
            order_created_at: Some(now_utc()),
            transaction_time: now_utc(),
            fill: Some(FillEvent {
                trade_id: trade_id.to_string(),
                order_id: "42".to_string(),
                symbol,
                side: Side::Buy,
                price,
                quantity,
                fee: Decimal::from_str_exact("0.01").unwrap(),
                fee_asset: "USDC".to_string(),
                fee_quote: Some(fee_quote),
                liquidity_side: domain::LiquiditySide::Taker,
                event_time: now_utc(),
            }),
            fill_recovery: None,
        }
    }

    #[tokio::test]
    async fn runtime_bootstraps_and_enters_trading() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();

        assert_eq!(runtime.state(), RuntimeState::Trading);
    }

    #[tokio::test]
    async fn bootstrap_recent_fills_do_not_double_count_inventory() {
        let gateway = MockBinanceSpotGateway::new();
        gateway
            .seed_account(sample_account_with_positions(
                Decimal::from_str_exact("0.005").unwrap(),
                Decimal::ZERO,
            ))
            .await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;
        gateway
            .seed_fills(
                Symbol::BtcUsdc,
                vec![FillEvent {
                    trade_id: "bootstrap-fill-1".to_string(),
                    order_id: "42".to_string(),
                    symbol: Symbol::BtcUsdc,
                    side: Side::Buy,
                    price: Decimal::from(60_000u32),
                    quantity: Decimal::from_str_exact("0.005").unwrap(),
                    fee: Decimal::from_str_exact("0.01").unwrap(),
                    fee_asset: "USDC".to_string(),
                    fee_quote: Some(Decimal::from_str_exact("0.01").unwrap()),
                    liquidity_side: domain::LiquiditySide::Taker,
                    event_time: now_utc(),
                }],
            )
            .await;

        let mut runtime = TraderRuntime::new(sample_config(false), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();

        let inventory = runtime.portfolio.inventory(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(inventory.base_position, Decimal::from_str_exact("0.005").unwrap());
        assert_eq!(inventory.average_entry_price, Some(Decimal::from(60_000u32)));
        assert_eq!(inventory.quote_position, Decimal::from(-300i32));
        assert!(runtime.seen_fills.contains(&FillKey {
            symbol: Symbol::BtcUsdc,
            trade_id: "bootstrap-fill-1".to_string(),
        }));
    }

    #[tokio::test]
    async fn pipeline_places_simple_orders_when_healthy() {
        let gateway = MockBinanceSpotGateway::new();
        gateway
            .seed_account(sample_account_with_positions(
                Decimal::from_str_exact("0.002").unwrap(),
                Decimal::ZERO,
            ))
            .await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
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
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();
        mark_streams_healthy(&gateway).await;
        runtime.run_cycle().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Trading);
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
        assert!(runtime
            .state_snapshot()
            .reason
            .contains("market_ws=Degraded"));
    }

    #[tokio::test]
    async fn runtime_recovers_when_user_stream_and_account_events_resume() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();
        mark_streams_healthy(&gateway).await;
        runtime.run_cycle().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Trading);

        gateway
            .emit_user_stream_status(StreamStatus::reconnecting(
                StreamKind::UserWs,
                1,
                "user stream reconnect test",
            ))
            .await;

        runtime.run_cycle().await.unwrap();

        assert_eq!(runtime.state(), RuntimeState::Reduced);
        assert_eq!(runtime.health().user_ws.state, HealthState::Degraded);

        gateway
            .emit_user_stream_status(StreamStatus::connected(
                StreamKind::UserWs,
                1,
                "user stream recovered",
            ))
            .await;
        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::AccountPosition {
                balances: sample_account().balances,
                updated_at: now_utc(),
            })
            .await;

        runtime.run_cycle().await.unwrap();

        assert_eq!(runtime.state(), RuntimeState::Trading);
        assert_eq!(runtime.health().user_ws.state, HealthState::Healthy);
        assert_eq!(runtime.health().account_events.state, HealthState::Healthy);
    }

    #[tokio::test]
    async fn pending_fill_recovery_moves_runtime_into_reconciling_and_back_to_trading() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();
        mark_streams_healthy(&gateway).await;
        runtime.run_cycle().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Trading);

        let mut execution = sample_execution_event(OrderStatus::PartiallyFilled);
        execution.fill = None;
        execution.fill_recovery = Some(BinanceFillRecoveryRequest {
            symbol: Symbol::BtcUsdc,
            trade_id: "runtime-recovery-fill".to_string(),
            order_id: "42".to_string(),
            fee_asset: "BNB".to_string(),
            event_time: now_utc(),
            reason: "fee quote enrichment failed; recover via REST".to_string(),
        });
        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(execution))
            .await;

        runtime.run_cycle().await.unwrap();

        assert_eq!(runtime.state(), RuntimeState::Reconciling);
        assert!(runtime.pending_fill_recoveries.contains_key(&FillKey {
            symbol: Symbol::BtcUsdc,
            trade_id: "runtime-recovery-fill".to_string(),
        }));
        assert!(runtime
            .state_snapshot()
            .reason
            .contains("pending_fill_recoveries=1"));

        gateway
            .seed_fills(
                Symbol::BtcUsdc,
                vec![FillEvent {
                    trade_id: "runtime-recovery-fill".to_string(),
                    order_id: "42".to_string(),
                    symbol: Symbol::BtcUsdc,
                    side: Side::Buy,
                    price: Decimal::from(60_000u32),
                    quantity: Decimal::from_str_exact("0.005").unwrap(),
                    fee: Decimal::from_str_exact("0.001").unwrap(),
                    fee_asset: "BNB".to_string(),
                    fee_quote: Some(Decimal::from_str_exact("0.6").unwrap()),
                    liquidity_side: domain::LiquiditySide::Taker,
                    event_time: now_utc(),
                }],
            )
            .await;
        mark_streams_healthy(&gateway).await;

        runtime.run_cycle().await.unwrap();

        assert_eq!(runtime.state(), RuntimeState::Trading);
        assert!(!runtime.pending_fill_recoveries.contains_key(&FillKey {
            symbol: Symbol::BtcUsdc,
            trade_id: "runtime-recovery-fill".to_string(),
        }));
    }

    #[tokio::test]
    async fn operator_pause_and_resume_transition_cleanly() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();
        mark_streams_healthy(&gateway).await;
        runtime.run_cycle().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Trading);

        let pause = runtime
            .apply_control_plane_request(ControlPlaneRequest {
                command: OperatorCommand::Pause,
                submitted_by: "operator".to_string(),
                submitted_at: now_utc(),
            })
            .await
            .unwrap();
        assert!(pause.accepted);
        assert_eq!(pause.resulting_state, RuntimeState::Paused);
        assert_eq!(runtime.state(), RuntimeState::Paused);

        mark_streams_healthy(&gateway).await;
        let resume = runtime
            .apply_control_plane_request(ControlPlaneRequest {
                command: OperatorCommand::Resume,
                submitted_by: "operator".to_string(),
                submitted_at: now_utc(),
            })
            .await
            .unwrap();
        assert!(resume.accepted);
        assert_eq!(resume.resulting_state, RuntimeState::Trading);
        assert_eq!(runtime.state(), RuntimeState::Trading);
    }

    #[tokio::test]
    async fn absorbable_cycle_error_enters_reconciling_without_risking_shutdown() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();
        mark_streams_healthy(&gateway).await;
        runtime.run_cycle().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Trading);

        runtime
            .handle_cycle_error(&anyhow::anyhow!("temporary reconciliation failure"))
            .await
            .unwrap();

        assert_eq!(runtime.state(), RuntimeState::Reconciling);
        assert!(runtime.state() != RuntimeState::RiskOff);
        mark_streams_healthy(&gateway).await;
        runtime.run_cycle().await.unwrap();
        assert_eq!(runtime.state(), RuntimeState::Trading);
    }

    #[tokio::test]
    async fn user_stream_execution_event_updates_execution_and_portfolio() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(false), gateway.clone()).unwrap();
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
    async fn runtime_recovers_pending_fill_via_recent_fills_rest() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(false), gateway.clone()).unwrap();
        runtime.bootstrap().await.unwrap();

        let recovered_fill = FillEvent {
            trade_id: "fill-1".to_string(),
            order_id: "42".to_string(),
            symbol: Symbol::BtcUsdc,
            side: Side::Buy,
            price: Decimal::from(60_000u32),
            quantity: Decimal::from_str_exact("0.005").unwrap(),
            fee: Decimal::from_str_exact("0.001").unwrap(),
            fee_asset: "BNB".to_string(),
            fee_quote: Some(Decimal::from_str_exact("0.6").unwrap()),
            liquidity_side: domain::LiquiditySide::Taker,
            event_time: now_utc(),
        };
        gateway.seed_fills(Symbol::BtcUsdc, vec![recovered_fill]).await;

        let mut execution = sample_execution_event(OrderStatus::PartiallyFilled);
        execution.fill = None;
        execution.fill_recovery = Some(BinanceFillRecoveryRequest {
            symbol: Symbol::BtcUsdc,
            trade_id: "fill-1".to_string(),
            order_id: "42".to_string(),
            fee_asset: "BNB".to_string(),
            event_time: now_utc(),
            reason: "fee quote enrichment failed; recover via REST".to_string(),
        });

        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(execution))
            .await;

        runtime.run_cycle().await.unwrap();

        let inventory = runtime.portfolio.inventory(Symbol::BtcUsdc).await.unwrap();
        let pnl = runtime.accounting.snapshot().await;
        assert!(inventory.base_position > Decimal::ZERO);
        assert_eq!(pnl.fees_quote, Decimal::from_str_exact("0.6").unwrap());
        assert!(!runtime
            .pending_fill_recoveries
            .contains_key(&FillKey {
                symbol: Symbol::BtcUsdc,
                trade_id: "fill-1".to_string(),
            }));
    }

    #[tokio::test]
    async fn fills_with_same_trade_id_on_different_symbols_are_distinct() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book_for(Symbol::BtcUsdc)).await;
        gateway.seed_orderbook(sample_book_for(Symbol::EthUsdc)).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.seed_trades(Symbol::EthUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(
            sample_config_with_pairs(
                false,
                vec![sample_pair(Symbol::BtcUsdc), sample_pair(Symbol::EthUsdc)],
            ),
            gateway.clone(),
        )
        .unwrap();
        runtime.bootstrap().await.unwrap();
        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(sample_execution_event_for(
                Symbol::BtcUsdc,
                "shared-trade-id",
                OrderStatus::PartiallyFilled,
            )))
            .await;
        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(sample_execution_event_for(
                Symbol::EthUsdc,
                "shared-trade-id",
                OrderStatus::PartiallyFilled,
            )))
            .await;

        runtime.run_cycle().await.unwrap();

        let btc_inventory = runtime.portfolio.inventory(Symbol::BtcUsdc).await.unwrap();
        let eth_inventory = runtime.portfolio.inventory(Symbol::EthUsdc).await.unwrap();
        let pnl = runtime.accounting.snapshot().await;
        assert!(btc_inventory.base_position > Decimal::ZERO);
        assert!(eth_inventory.base_position > Decimal::ZERO);
        assert_eq!(pnl.fees_quote, Decimal::from_str_exact("0.02").unwrap());
        assert!(runtime.seen_fills.contains(&FillKey {
            symbol: Symbol::BtcUsdc,
            trade_id: "shared-trade-id".to_string(),
        }));
        assert!(runtime.seen_fills.contains(&FillKey {
            symbol: Symbol::EthUsdc,
            trade_id: "shared-trade-id".to_string(),
        }));
    }

    #[tokio::test]
    async fn pending_recoveries_with_same_trade_id_on_different_symbols_do_not_collide() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book_for(Symbol::BtcUsdc)).await;
        gateway.seed_orderbook(sample_book_for(Symbol::EthUsdc)).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.seed_trades(Symbol::EthUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(
            sample_config_with_pairs(
                false,
                vec![sample_pair(Symbol::BtcUsdc), sample_pair(Symbol::EthUsdc)],
            ),
            gateway.clone(),
        )
        .unwrap();
        runtime.bootstrap().await.unwrap();

        let mut btc_execution =
            sample_execution_event_for(Symbol::BtcUsdc, "shared-recovery-id", OrderStatus::PartiallyFilled);
        btc_execution.fill = None;
        btc_execution.fill_recovery = Some(BinanceFillRecoveryRequest {
            symbol: Symbol::BtcUsdc,
            trade_id: "shared-recovery-id".to_string(),
            order_id: "42".to_string(),
            fee_asset: "BNB".to_string(),
            event_time: now_utc(),
            reason: "recover btc".to_string(),
        });

        let mut eth_execution =
            sample_execution_event_for(Symbol::EthUsdc, "shared-recovery-id", OrderStatus::PartiallyFilled);
        eth_execution.fill = None;
        eth_execution.fill_recovery = Some(BinanceFillRecoveryRequest {
            symbol: Symbol::EthUsdc,
            trade_id: "shared-recovery-id".to_string(),
            order_id: "42".to_string(),
            fee_asset: "BNB".to_string(),
            event_time: now_utc(),
            reason: "recover eth".to_string(),
        });

        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(btc_execution))
            .await;
        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(eth_execution))
            .await;

        runtime.run_cycle().await.unwrap();

        assert_eq!(runtime.pending_fill_recoveries.len(), 2);
        assert!(runtime.pending_fill_recoveries.contains_key(&FillKey {
            symbol: Symbol::BtcUsdc,
            trade_id: "shared-recovery-id".to_string(),
        }));
        assert!(runtime.pending_fill_recoveries.contains_key(&FillKey {
            symbol: Symbol::EthUsdc,
            trade_id: "shared-recovery-id".to_string(),
        }));
    }

    #[tokio::test]
    async fn rest_recovery_resolves_exact_matching_symbol_and_trade_id() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book_for(Symbol::BtcUsdc)).await;
        gateway.seed_orderbook(sample_book_for(Symbol::EthUsdc)).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.seed_trades(Symbol::EthUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(
            sample_config_with_pairs(
                false,
                vec![sample_pair(Symbol::BtcUsdc), sample_pair(Symbol::EthUsdc)],
            ),
            gateway.clone(),
        )
        .unwrap();
        runtime.bootstrap().await.unwrap();

        gateway
            .seed_fills(
                Symbol::BtcUsdc,
                vec![FillEvent {
                    trade_id: "shared-recovery-id".to_string(),
                    order_id: "42".to_string(),
                    symbol: Symbol::BtcUsdc,
                    side: Side::Buy,
                    price: Decimal::from(60_000u32),
                    quantity: Decimal::from_str_exact("0.005").unwrap(),
                    fee: Decimal::from_str_exact("0.001").unwrap(),
                    fee_asset: "BNB".to_string(),
                    fee_quote: Some(Decimal::from_str_exact("0.6").unwrap()),
                    liquidity_side: domain::LiquiditySide::Taker,
                    event_time: now_utc(),
                }],
            )
            .await;

        let mut btc_execution =
            sample_execution_event_for(Symbol::BtcUsdc, "shared-recovery-id", OrderStatus::PartiallyFilled);
        btc_execution.fill = None;
        btc_execution.fill_recovery = Some(BinanceFillRecoveryRequest {
            symbol: Symbol::BtcUsdc,
            trade_id: "shared-recovery-id".to_string(),
            order_id: "42".to_string(),
            fee_asset: "BNB".to_string(),
            event_time: now_utc(),
            reason: "recover btc".to_string(),
        });

        let mut eth_execution =
            sample_execution_event_for(Symbol::EthUsdc, "shared-recovery-id", OrderStatus::PartiallyFilled);
        eth_execution.fill = None;
        eth_execution.fill_recovery = Some(BinanceFillRecoveryRequest {
            symbol: Symbol::EthUsdc,
            trade_id: "shared-recovery-id".to_string(),
            order_id: "42".to_string(),
            fee_asset: "BNB".to_string(),
            event_time: now_utc(),
            reason: "recover eth".to_string(),
        });

        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(btc_execution))
            .await;
        gateway
            .emit_user_stream_event(BinanceUserStreamEvent::Execution(eth_execution))
            .await;

        runtime.run_cycle().await.unwrap();

        let btc_inventory = runtime.portfolio.inventory(Symbol::BtcUsdc).await.unwrap();
        let eth_inventory = runtime.portfolio.inventory(Symbol::EthUsdc).await.unwrap();
        assert!(btc_inventory.base_position > Decimal::ZERO);
        assert_eq!(eth_inventory.base_position, Decimal::ZERO);
        assert!(!runtime.pending_fill_recoveries.contains_key(&FillKey {
            symbol: Symbol::BtcUsdc,
            trade_id: "shared-recovery-id".to_string(),
        }));
        assert!(runtime.pending_fill_recoveries.contains_key(&FillKey {
            symbol: Symbol::EthUsdc,
            trade_id: "shared-recovery-id".to_string(),
        }));
    }

    #[tokio::test]
    async fn gap_in_market_delta_triggers_resync_from_rest_snapshot() {
        let gateway = MockBinanceSpotGateway::new();
        gateway.seed_account(sample_account()).await;
        gateway.seed_orderbook(sample_book()).await;
        gateway.seed_trades(Symbol::BtcUsdc, sample_trades()).await;
        gateway.set_clock_time(now_utc()).await;

        let mut runtime = TraderRuntime::new(sample_config(true), gateway.clone()).unwrap();
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
