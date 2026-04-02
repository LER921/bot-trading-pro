use anyhow::Result;
use async_trait::async_trait;
use common::{ids::new_client_order_id, now_utc};
use domain::{
    ExecutionReport, OpenOrder, OrderRequest, OrderStatus, OrderType, RiskDecision, RiskMode,
    Symbol, TradeIntent,
};
use exchange_binance_spot::{BinanceExecutionEvent, BinanceSpotGateway};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[async_trait]
pub trait ExecutionEngine: Send + Sync {
    async fn execute(&self, decision: RiskDecision) -> Result<Option<ExecutionReport>>;
    async fn cancel_all(&self, symbol: Option<Symbol>) -> Result<()>;
    async fn reconcile(&self) -> Result<Vec<OpenOrder>>;
    async fn sync_open_orders(&self, open_orders: Vec<OpenOrder>) -> Result<Vec<OpenOrder>>;
    async fn apply_execution_event(&self, event: &BinanceExecutionEvent) -> Result<()>;
    async fn open_orders(&self) -> Vec<OpenOrder>;
}

#[derive(Debug, Default)]
struct ExecutionState {
    open_orders: HashMap<String, OpenOrder>,
    processed_intents: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct BinanceExecutionEngine<G> {
    gateway: G,
    symbols: Vec<Symbol>,
    state: Arc<RwLock<ExecutionState>>,
}

impl<G> BinanceExecutionEngine<G> {
    pub fn new(gateway: G, symbols: Vec<Symbol>) -> Self {
        Self {
            gateway,
            symbols,
            state: Arc::new(RwLock::new(ExecutionState::default())),
        }
    }
}

#[async_trait]
impl<G> ExecutionEngine for BinanceExecutionEngine<G>
where
    G: BinanceSpotGateway + Send + Sync,
{
    async fn execute(&self, decision: RiskDecision) -> Result<Option<ExecutionReport>> {
        if matches!(
            decision.resulting_mode,
            RiskMode::Paused | RiskMode::RiskOff | RiskMode::ReduceOnly
        ) && decision.approved_intent.is_none()
        {
            info!(reason = decision.reason, "execution skipped because risk denied the intent");
            return Ok(None);
        }

        let Some(intent) = decision.approved_intent.as_ref() else {
            info!(reason = decision.reason, "execution received no approved intent");
            return Ok(None);
        };

        {
            let state = self.state.read().await;
            if state.processed_intents.contains(&intent.intent_id) {
                warn!(intent_id = intent.intent_id, "duplicate intent skipped by execution engine");
                return Ok(None);
            }
        }

        let request = build_order_request(intent);
        info!(
            symbol = %request.symbol,
            client_order_id = request.client_order_id,
            reduce_only = request.reduce_only,
            "sending order to exchange"
        );

        let report = self.gateway.place_order(request.clone()).await?;

        let mut state = self.state.write().await;
        state.processed_intents.insert(intent.intent_id.clone());

        if let Some(open_order) = report.to_open_order(&request) {
            state
                .open_orders
                .insert(open_order.client_order_id.clone(), open_order);
        } else {
            state.open_orders.remove(&report.client_order_id);
        }

        info!(
            status = ?report.status,
            client_order_id = report.client_order_id,
            "exchange execution report received"
        );
        Ok(Some(report))
    }

    async fn cancel_all(&self, symbol: Option<Symbol>) -> Result<()> {
        match symbol {
            Some(symbol) => {
                self.gateway.cancel_all_orders(symbol).await?;
                self.state
                    .write()
                    .await
                    .open_orders
                    .retain(|_, order| order.symbol != symbol);
            }
            None => {
                for &symbol in &self.symbols {
                    self.gateway.cancel_all_orders(symbol).await?;
                }
                self.state.write().await.open_orders.clear();
            }
        }

        info!(?symbol, "cancel-all completed");
        Ok(())
    }

    async fn reconcile(&self) -> Result<Vec<OpenOrder>> {
        let mut reconciled = Vec::new();
        for &symbol in &self.symbols {
            reconciled.extend(self.gateway.fetch_open_orders(symbol).await?);
        }

        self.sync_open_orders(reconciled).await
    }

    async fn sync_open_orders(&self, open_orders: Vec<OpenOrder>) -> Result<Vec<OpenOrder>> {
        let mut state = self.state.write().await;
        state.open_orders = open_orders
            .iter()
            .cloned()
            .map(|order| (order.client_order_id.clone(), order))
            .collect();

        info!(open_orders = state.open_orders.len(), "execution reconciliation completed");
        Ok(state.open_orders.values().cloned().collect())
    }

    async fn apply_execution_event(&self, event: &BinanceExecutionEvent) -> Result<()> {
        let mut state = self.state.write().await;
        if event.report.status.is_open() {
            state.open_orders.insert(
                event.report.client_order_id.clone(),
                OpenOrder {
                    client_order_id: event.report.client_order_id.clone(),
                    exchange_order_id: event.report.exchange_order_id.clone(),
                    symbol: event.report.symbol,
                    side: event.side,
                    price: event.price,
                    original_quantity: event.original_quantity,
                    executed_quantity: event.cumulative_filled_quantity,
                    status: event.report.status,
                    reduce_only: false,
                    updated_at: event.transaction_time,
                },
            );
        } else {
            state.open_orders.remove(&event.report.client_order_id);
        }

        info!(
            client_order_id = event.report.client_order_id,
            status = ?event.report.status,
            "execution state updated from user stream event"
        );
        Ok(())
    }

    async fn open_orders(&self) -> Vec<OpenOrder> {
        self.state
            .read()
            .await
            .open_orders
            .values()
            .cloned()
            .collect()
    }
}

fn build_order_request(intent: &TradeIntent) -> OrderRequest {
    OrderRequest {
        client_order_id: new_client_order_id("bot"),
        symbol: intent.symbol,
        side: intent.side,
        order_type: if intent.post_only {
            OrderType::LimitMaker
        } else if intent.limit_price.is_some() {
            OrderType::Limit
        } else {
            OrderType::Market
        },
        price: intent.limit_price,
        quantity: intent.quantity,
        time_in_force: intent.time_in_force,
        post_only: intent.post_only,
        reduce_only: intent.reduce_only,
        source_intent_id: intent.intent_id.clone(),
    }
}

#[allow(dead_code)]
fn _rejected_report(request: &OrderRequest) -> ExecutionReport {
    ExecutionReport {
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: None,
        symbol: request.symbol,
        status: OrderStatus::Rejected,
        filled_quantity: common::Decimal::ZERO,
        average_fill_price: None,
        message: Some("execution rejection".to_string()),
        event_time: now_utc(),
    }
}
