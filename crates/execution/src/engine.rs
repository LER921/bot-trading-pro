use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, ids::new_client_order_id, now_utc};
use common::retry::{RetryPolicy, retry_async};
use domain::{
    ExecutionReport, ExecutionStats, OpenOrder, OrderRequest, OrderStatus, OrderType, RiskDecision,
    RiskMode, Symbol, TradeIntent,
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
    async fn cancel_stale_orders(&self, max_age_ms: i64, mid_prices: &HashMap<Symbol, Decimal>) -> Result<usize>;
    async fn reconcile(&self) -> Result<Vec<OpenOrder>>;
    async fn sync_open_orders(&self, open_orders: Vec<OpenOrder>) -> Result<Vec<OpenOrder>>;
    async fn apply_execution_event(&self, event: &BinanceExecutionEvent) -> Result<()>;
    async fn stats(&self) -> ExecutionStats;
    async fn open_orders(&self) -> Vec<OpenOrder>;
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    request: OrderRequest,
}

#[derive(Debug, Default)]
struct ExecutionAggregate {
    total_submitted: u64,
    total_rejected: u64,
    total_canceled: u64,
    total_filled_reports: u64,
    total_stale_cancels: u64,
    fill_ratio_sum: Decimal,
    fill_ratio_samples: u64,
    slippage_bps_sum: Decimal,
    slippage_bps_samples: u64,
    decision_latency_ms_sum: Decimal,
    decision_latency_samples: u64,
}

#[derive(Debug, Default)]
struct ExecutionState {
    open_orders: HashMap<String, OpenOrder>,
    processed_intents: HashSet<String>,
    tracked_orders: HashMap<String, TrackedOrder>,
    aggregate: ExecutionAggregate,
}

#[derive(Debug, Clone)]
pub struct BinanceExecutionEngine<G> {
    gateway: G,
    symbols: Vec<Symbol>,
    retry_policy: RetryPolicy,
    state: Arc<RwLock<ExecutionState>>,
}

impl<G> BinanceExecutionEngine<G> {
    pub fn new(gateway: G, symbols: Vec<Symbol>) -> Self {
        Self {
            gateway,
            symbols,
            retry_policy: RetryPolicy {
                max_attempts: 2,
                initial_backoff_ms: 100,
                max_backoff_ms: 500,
            },
            state: Arc::new(RwLock::new(ExecutionState::default())),
        }
    }
}

#[async_trait]
impl<G> ExecutionEngine for BinanceExecutionEngine<G>
where
    G: BinanceSpotGateway + Clone + Send + Sync,
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
        let request_for_retry = request.clone();
        let gateway = self.gateway.clone();
        info!(
            symbol = %request.symbol,
            client_order_id = request.client_order_id,
            reduce_only = request.reduce_only,
            "sending order to exchange"
        );

        let raw_report = retry_async(&self.retry_policy, |_| {
            let gateway = gateway.clone();
            let request = request_for_retry.clone();
            async move { gateway.place_order(request).await }
        })
        .await?;
        let report = enrich_report(raw_report, &request, intent.created_at);

        let mut state = self.state.write().await;
        state.processed_intents.insert(intent.intent_id.clone());
        state.aggregate.total_submitted = state.aggregate.total_submitted.saturating_add(1);
        update_aggregate_from_report(&mut state.aggregate, &report);

        if report.status.is_open() {
            if let Some(open_order) = report.to_open_order(&request) {
                state
                    .open_orders
                    .insert(open_order.client_order_id.clone(), open_order);
            }
            state.tracked_orders.insert(
                report.client_order_id.clone(),
                TrackedOrder {
                    request,
                },
            );
        } else {
            state.open_orders.remove(&report.client_order_id);
            state.tracked_orders.remove(&report.client_order_id);
        }

        info!(
            status = ?report.status,
            client_order_id = report.client_order_id,
            fill_ratio = %report.fill_ratio,
            slippage_bps = ?report.slippage_bps,
            "exchange execution report received"
        );
        Ok(Some(report))
    }

    async fn cancel_all(&self, symbol: Option<Symbol>) -> Result<()> {
        let canceled = match symbol {
            Some(symbol) => {
                self.gateway.cancel_all_orders(symbol).await?;
                let mut state = self.state.write().await;
                let count = state
                    .open_orders
                    .values()
                    .filter(|order| order.symbol == symbol)
                    .count() as u64;
                state.open_orders.retain(|_, order| order.symbol != symbol);
                state.tracked_orders.retain(|_, order| order.request.symbol != symbol);
                state.aggregate.total_canceled += count;
                count
            }
            None => {
                for &symbol in &self.symbols {
                    self.gateway.cancel_all_orders(symbol).await?;
                }
                let mut state = self.state.write().await;
                let count = state.open_orders.len() as u64;
                state.open_orders.clear();
                state.tracked_orders.clear();
                state.aggregate.total_canceled += count;
                count
            }
        };

        info!(?symbol, canceled, "cancel-all completed");
        Ok(())
    }

    async fn cancel_stale_orders(&self, max_age_ms: i64, mid_prices: &HashMap<Symbol, Decimal>) -> Result<usize> {
        let now = now_utc();
        let stale_symbols = {
            let state = self.state.read().await;
            state
                .open_orders
                .values()
                .filter(|order| {
                    let age_ms = (now - order.updated_at).whole_milliseconds() as i64;
                    if age_ms > max_age_ms {
                        return true;
                    }

                    let Some(order_price) = order.price else {
                        return false;
                    };
                    let Some(mid_price) = mid_prices.get(&order.symbol).copied() else {
                        return false;
                    };
                    let distance_bps = if mid_price.is_zero() {
                        Decimal::ZERO
                    } else {
                        ((order_price - mid_price).abs() / mid_price) * Decimal::from(10_000u32)
                    };
                    distance_bps > Decimal::from(12u32)
                })
                .map(|order| order.symbol)
                .collect::<HashSet<_>>()
        };

        let mut canceled = 0usize;
        for symbol in stale_symbols {
            self.gateway.cancel_all_orders(symbol).await?;
            let mut state = self.state.write().await;
            let count = state
                .open_orders
                .values()
                .filter(|order| order.symbol == symbol)
                .count();
            if count > 0 {
                canceled += count;
                state.aggregate.total_canceled += count as u64;
                state.aggregate.total_stale_cancels += count as u64;
            }
            state.open_orders.retain(|_, order| order.symbol != symbol);
            state.tracked_orders.retain(|_, order| order.request.symbol != symbol);
        }

        Ok(canceled)
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
        let open_ids = state.open_orders.keys().cloned().collect::<HashSet<_>>();
        state
            .tracked_orders
            .retain(|client_order_id, _| open_ids.contains(client_order_id));

        info!(open_orders = state.open_orders.len(), "execution reconciliation completed");
        Ok(state.open_orders.values().cloned().collect())
    }

    async fn apply_execution_event(&self, event: &BinanceExecutionEvent) -> Result<()> {
        let mut state = self.state.write().await;
        update_aggregate_from_report(&mut state.aggregate, &event.report);

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
            state.tracked_orders.entry(event.report.client_order_id.clone()).or_insert(
                TrackedOrder {
                    request: OrderRequest {
                        client_order_id: event.report.client_order_id.clone(),
                        symbol: event.report.symbol,
                        side: event.side,
                        order_type: event.order_type,
                        price: event.price,
                        quantity: event.original_quantity,
                        time_in_force: event.time_in_force,
                        post_only: matches!(event.order_type, OrderType::LimitMaker),
                        reduce_only: false,
                        source_intent_id: "user-stream".to_string(),
                    },
                },
            );
        } else {
            if matches!(event.report.status, OrderStatus::Canceled) {
                state.aggregate.total_canceled = state.aggregate.total_canceled.saturating_add(1);
            }
            state.open_orders.remove(&event.report.client_order_id);
            state.tracked_orders.remove(&event.report.client_order_id);
        }

        info!(
            client_order_id = event.report.client_order_id,
            status = ?event.report.status,
            "execution state updated from user stream event"
        );
        Ok(())
    }

    async fn stats(&self) -> ExecutionStats {
        let state = self.state.read().await;
        build_stats(&state.aggregate)
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

fn enrich_report(mut report: ExecutionReport, request: &OrderRequest, intent_created_at: common::Timestamp) -> ExecutionReport {
    report.requested_price = request.price;
    report.fill_ratio = if request.quantity.is_zero() {
        Decimal::ZERO
    } else {
        report.filled_quantity / request.quantity
    };
    report.slippage_bps = match (report.average_fill_price, request.price) {
        (Some(fill_price), Some(request_price)) if !request_price.is_zero() => Some(
            ((fill_price - request_price).abs() / request_price) * Decimal::from(10_000u32),
        ),
        _ => None,
    };
    report.decision_latency_ms = Some((report.event_time - intent_created_at).whole_milliseconds() as i64);
    report
}

fn update_aggregate_from_report(aggregate: &mut ExecutionAggregate, report: &ExecutionReport) {
    if matches!(report.status, OrderStatus::Rejected) {
        aggregate.total_rejected = aggregate.total_rejected.saturating_add(1);
    }

    if matches!(report.status, OrderStatus::Filled | OrderStatus::PartiallyFilled) {
        aggregate.total_filled_reports = aggregate.total_filled_reports.saturating_add(1);
    }

    aggregate.fill_ratio_sum += report.fill_ratio;
    aggregate.fill_ratio_samples = aggregate.fill_ratio_samples.saturating_add(1);

    if let Some(slippage_bps) = report.slippage_bps {
        aggregate.slippage_bps_sum += slippage_bps;
        aggregate.slippage_bps_samples = aggregate.slippage_bps_samples.saturating_add(1);
    }

    if let Some(latency_ms) = report.decision_latency_ms {
        aggregate.decision_latency_ms_sum += Decimal::from(latency_ms);
        aggregate.decision_latency_samples = aggregate.decision_latency_samples.saturating_add(1);
    }
}

fn build_stats(aggregate: &ExecutionAggregate) -> ExecutionStats {
    let updated_at = now_utc();
    ExecutionStats {
        total_submitted: aggregate.total_submitted,
        total_rejected: aggregate.total_rejected,
        total_canceled: aggregate.total_canceled,
        total_filled_reports: aggregate.total_filled_reports,
        total_stale_cancels: aggregate.total_stale_cancels,
        reject_rate: if aggregate.total_submitted == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(aggregate.total_rejected) / Decimal::from(aggregate.total_submitted)
        },
        avg_fill_ratio: if aggregate.fill_ratio_samples == 0 {
            Decimal::ZERO
        } else {
            aggregate.fill_ratio_sum / Decimal::from(aggregate.fill_ratio_samples)
        },
        avg_slippage_bps: if aggregate.slippage_bps_samples == 0 {
            Decimal::ZERO
        } else {
            aggregate.slippage_bps_sum / Decimal::from(aggregate.slippage_bps_samples)
        },
        avg_decision_latency_ms: if aggregate.decision_latency_samples == 0 {
            Decimal::ZERO
        } else {
            aggregate.decision_latency_ms_sum / Decimal::from(aggregate.decision_latency_samples)
        },
        updated_at,
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
        fill_ratio: Decimal::ZERO,
        requested_price: request.price,
        slippage_bps: None,
        decision_latency_ms: None,
        message: Some("execution rejection".to_string()),
        event_time: now_utc(),
    }
}
