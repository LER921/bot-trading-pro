use anyhow::Result;
use async_trait::async_trait;
use common::retry::{RetryPolicy, retry_async};
use common::{Decimal, ids::new_client_order_id, now_utc};
use domain::{
    ExecutionReport, ExecutionStats, OpenOrder, OrderRequest, OrderStatus, OrderType, RiskDecision,
    RiskMode, Symbol, TimeInForce, TradeIntent,
};
use exchange_binance_spot::{BinanceExecutionEvent, BinanceSpotGateway, PreparedOrder};
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
    async fn apply_execution_event(&self, event: &BinanceExecutionEvent) -> Result<ExecutionReport>;
    async fn stats(&self) -> ExecutionStats;
    async fn open_orders(&self) -> Vec<OpenOrder>;
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    request: OrderRequest,
    exchange_order_id: Option<String>,
    status: OrderStatus,
    filled_quantity: Decimal,
    updated_at: common::Timestamp,
    submit_started_at: Option<common::Timestamp>,
    decision_latency_ms: Option<i64>,
    submit_ack_latency_ms: Option<i64>,
    first_report_observed_at: Option<common::Timestamp>,
    first_fill_observed_at: Option<common::Timestamp>,
}

impl TrackedOrder {
    fn from_request(
        request: OrderRequest,
        report: &ExecutionReport,
        submit_started_at: Option<common::Timestamp>,
        observed_at: Option<common::Timestamp>,
    ) -> Self {
        Self {
            request,
            exchange_order_id: report.exchange_order_id.clone(),
            status: report.status,
            filled_quantity: report.filled_quantity,
            updated_at: report.event_time,
            submit_started_at,
            decision_latency_ms: report.decision_latency_ms,
            submit_ack_latency_ms: report.submit_ack_latency_ms,
            first_report_observed_at: None,
            first_fill_observed_at: if report.filled_quantity > Decimal::ZERO {
                observed_at
            } else {
                None
            },
        }
    }

    fn from_open_order(order: &OpenOrder) -> Self {
        Self {
            request: OrderRequest {
                client_order_id: order.client_order_id.clone(),
                symbol: order.symbol,
                side: order.side,
                order_type: if order.price.is_some() {
                    OrderType::Limit
                } else {
                    OrderType::Market
                },
                price: order.price,
                quantity: order.original_quantity,
                time_in_force: Some(TimeInForce::Gtc),
                post_only: false,
                reduce_only: order.reduce_only,
                intent_role: order.intent_role,
                exit_stage: order.exit_stage,
                exit_reason: order.exit_reason.clone(),
                edge_after_cost_bps: None,
                expected_realized_edge_bps: None,
                adverse_selection_penalty_bps: None,
                source_intent_id: format!("reconciled-{}", order.client_order_id),
            },
            exchange_order_id: order.exchange_order_id.clone(),
            status: order.status,
            filled_quantity: order.executed_quantity,
            updated_at: order.updated_at,
            submit_started_at: None,
            decision_latency_ms: None,
            submit_ack_latency_ms: None,
            first_report_observed_at: None,
            first_fill_observed_at: None,
        }
    }

    fn merge_open_order(&mut self, order: &OpenOrder) {
        self.request.symbol = order.symbol;
        self.request.side = order.side;
        self.request.price = order.price.or(self.request.price);
        self.request.quantity = order.original_quantity;
        self.request.reduce_only = order.reduce_only;
        self.exchange_order_id = order.exchange_order_id.clone();
        self.status = order.status;
        self.filled_quantity = order.executed_quantity;
        self.updated_at = order.updated_at;
    }

    fn merge_event_fields(&mut self, event: &BinanceExecutionEvent) {
        self.request.symbol = event.report.symbol;
        self.request.side = event.side;
        self.request.order_type = event.order_type;
        self.request.price = event.price.or(self.request.price);
        if self.request.quantity < event.original_quantity {
            self.request.quantity = event.original_quantity;
        }
        if self.request.time_in_force.is_none() {
            self.request.time_in_force = event.time_in_force;
        }
        self.request.post_only = matches!(event.order_type, OrderType::LimitMaker);
    }

    fn update_from_report(&mut self, report: &ExecutionReport) -> bool {
        let progressed = self.status != report.status
            || self.filled_quantity != report.filled_quantity
            || self.exchange_order_id != report.exchange_order_id;
        self.exchange_order_id = report.exchange_order_id.clone();
        self.status = report.status;
        self.filled_quantity = report.filled_quantity;
        self.updated_at = report.event_time;
        progressed
    }

    fn to_open_order(&self) -> Option<OpenOrder> {
        self.status.is_open().then(|| OpenOrder {
            client_order_id: self.request.client_order_id.clone(),
            exchange_order_id: self.exchange_order_id.clone(),
            symbol: self.request.symbol,
            side: self.request.side,
            price: self.request.price,
            original_quantity: self.request.quantity,
            executed_quantity: self.filled_quantity,
            status: self.status,
            reduce_only: self.request.reduce_only,
            intent_role: self.request.intent_role,
            exit_stage: self.request.exit_stage,
            exit_reason: self.request.exit_reason.clone(),
            updated_at: self.updated_at,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingCancelKind {
    Manual,
    Stale,
}

#[derive(Debug, Clone, Copy)]
struct StaleOrderEvaluation {
    cancel_candidate: bool,
    keep_worthy: bool,
    hard_stale: bool,
}

#[derive(Debug, Default)]
struct ExecutionAggregate {
    total_submitted: u64,
    total_rejected: u64,
    total_local_validation_rejects: u64,
    total_benign_exchange_rejected: u64,
    risk_scored_rejections: u64,
    total_canceled: u64,
    total_manual_cancels: u64,
    total_external_cancels: u64,
    total_filled_reports: u64,
    total_stale_cancels: u64,
    total_duplicate_intents: u64,
    total_equivalent_order_skips: u64,
    fill_ratio_sum: Decimal,
    fill_ratio_samples: u64,
    slippage_bps_sum: Decimal,
    slippage_bps_samples: u64,
    decision_latency_ms_sum: Decimal,
    decision_latency_samples: u64,
    submit_ack_latency_ms_sum: Decimal,
    submit_ack_latency_samples: u64,
    submit_to_first_report_ms_sum: Decimal,
    submit_to_first_report_samples: u64,
    submit_to_fill_ms_sum: Decimal,
    submit_to_fill_samples: u64,
    cancel_ack_latency_ms_sum: Decimal,
    cancel_ack_latency_samples: u64,
}

#[derive(Debug, Default)]
struct ExecutionState {
    open_orders: HashMap<String, OpenOrder>,
    processed_intents: HashSet<String>,
    tracked_orders: HashMap<String, TrackedOrder>,
    pending_cancel_reports: HashMap<String, PendingCancelKind>,
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

        let original_request = build_order_request(intent);
        let request = match self.gateway.prepare_order(original_request.clone()).await? {
            PreparedOrder::Ready(request) => request,
            PreparedOrder::Rejected(raw_report) => {
                let report =
                    enrich_report(raw_report, &original_request, intent.created_at, None, now_utc());
                let mut state = self.state.write().await;
                state.processed_intents.insert(intent.intent_id.clone());
                update_aggregate_from_report(&mut state.aggregate, &report);
                record_decision_latency(&mut state.aggregate, report.decision_latency_ms);
                warn!(
                    symbol = %report.symbol,
                    client_order_id = report.client_order_id,
                    message = ?report.message,
                    "execution rejected order locally before exchange submit"
                );
                return Ok(Some(report));
            }
        };
        {
            let mut state = self.state.write().await;
            if state.processed_intents.contains(&intent.intent_id) {
                state.aggregate.total_duplicate_intents =
                    state.aggregate.total_duplicate_intents.saturating_add(1);
                warn!(intent_id = intent.intent_id, "duplicate intent skipped by execution engine");
                return Ok(None);
            }

            if let Some(existing_client_order_id) = find_equivalent_open_order(&state, &request) {
                state.processed_intents.insert(intent.intent_id.clone());
                state.aggregate.total_equivalent_order_skips =
                    state.aggregate.total_equivalent_order_skips.saturating_add(1);
                info!(
                    symbol = %request.symbol,
                    client_order_id = request.client_order_id,
                    existing_client_order_id,
                    source_intent_id = request.source_intent_id,
                    "execution skipped because an equivalent live order is already working"
                );
                return Ok(None);
            }
        }

        let request_for_retry = request.clone();
        let gateway = self.gateway.clone();
        info!(
            symbol = %request.symbol,
            client_order_id = request.client_order_id,
            reduce_only = request.reduce_only,
            "sending order to exchange"
        );

        let submit_started_at = now_utc();
        let raw_report = retry_async(&self.retry_policy, |_| {
            let gateway = gateway.clone();
            let request = request_for_retry.clone();
            async move { gateway.place_order(request).await }
        })
        .await?;
        let ack_observed_at = now_utc();
        let report = enrich_report(
            raw_report,
            &request,
            intent.created_at,
            Some(submit_started_at),
            ack_observed_at,
        );

        let mut state = self.state.write().await;
        state.processed_intents.insert(intent.intent_id.clone());
        state.aggregate.total_submitted = state.aggregate.total_submitted.saturating_add(1);
        update_aggregate_from_report(&mut state.aggregate, &report);
        record_decision_latency(&mut state.aggregate, report.decision_latency_ms);
        record_submit_ack_latency(&mut state.aggregate, report.submit_ack_latency_ms);
        record_submit_to_fill_latency(&mut state.aggregate, report.submit_to_fill_ms);
        upsert_state_from_request_report(
            &mut state,
            request,
            &report,
            Some(submit_started_at),
            Some(ack_observed_at),
        );

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
        let orders_to_cancel = {
            let state = self.state.read().await;
            state
                .open_orders
                .values()
                .filter(|order| {
                    is_bot_managed_client_order_id(&order.client_order_id)
                        && symbol.map(|value| value == order.symbol).unwrap_or(true)
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        for order in &orders_to_cancel {
            let cancel_started_at = now_utc();
            self.gateway.cancel_order(order).await?;
            let cancel_observed_at = now_utc();
            let cancel_ack_latency_ms =
                (cancel_observed_at - cancel_started_at).whole_milliseconds() as i64;
            let mut state = self.state.write().await;
            record_cancel_ack_latency(&mut state.aggregate, Some(cancel_ack_latency_ms));
        }

        let canceled = {
            let mut state = self.state.write().await;
            orders_to_cancel
                .iter()
                .filter(|order| {
                    mark_order_canceled(
                        &mut state,
                        &order.client_order_id,
                        PendingCancelKind::Manual,
                    )
                })
                .count()
        };

        info!(?symbol, canceled, cancel_kind = "manual", "cancel-all completed");
        Ok(())
    }

    async fn cancel_stale_orders(&self, max_age_ms: i64, mid_prices: &HashMap<Symbol, Decimal>) -> Result<usize> {
        let now = now_utc();
        let stale_orders = {
            let state = self.state.read().await;
            state
                .open_orders
                .values()
                .filter(|order| is_bot_managed_client_order_id(&order.client_order_id))
                .filter_map(|order| {
                    let evaluation =
                        evaluate_stale_order(order, max_age_ms, now, mid_prices.get(&order.symbol).copied());
                    should_cancel_order(evaluation).then_some(order.clone())
                })
                .collect::<Vec<_>>()
        };

        let mut canceled = 0usize;
        for order in stale_orders {
            let cancel_started_at = now_utc();
            self.gateway.cancel_order(&order).await?;
            let cancel_observed_at = now_utc();
            let count = {
                let mut state = self.state.write().await;
                let cancel_ack_latency_ms =
                    (cancel_observed_at - cancel_started_at).whole_milliseconds() as i64;
                record_cancel_ack_latency(&mut state.aggregate, Some(cancel_ack_latency_ms));
                usize::from(mark_order_canceled(
                    &mut state,
                    &order.client_order_id,
                    PendingCancelKind::Stale,
                ))
            };
            if count != 0 {
                info!(
                    symbol = %order.symbol,
                    client_order_id = %order.client_order_id,
                    canceled = count,
                    cancel_kind = "stale",
                    "stale bot order canceled after execution-quality evaluation"
                );
            }
            canceled += count;
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
        let mut previous_tracked = std::mem::take(&mut state.tracked_orders);
        let mut next_open_orders = HashMap::new();
        let mut next_tracked = HashMap::new();

        for order in open_orders {
            let mut tracked = previous_tracked
                .remove(&order.client_order_id)
                .unwrap_or_else(|| TrackedOrder::from_open_order(&order));
            tracked.merge_open_order(&order);

            if let Some(kind) = state.pending_cancel_reports.remove(&order.client_order_id) {
                undo_pending_cancel(&mut state.aggregate, kind);
            }

            if let Some(open_order) = tracked.to_open_order() {
                next_open_orders.insert(open_order.client_order_id.clone(), open_order);
            }
            next_tracked.insert(order.client_order_id.clone(), tracked);
        }

        state.open_orders = next_open_orders;
        state.tracked_orders = next_tracked;

        let bot_open_orders = state
            .open_orders
            .values()
            .filter(|order| is_bot_managed_client_order_id(&order.client_order_id))
            .count();
        let external_open_orders = state.open_orders.len().saturating_sub(bot_open_orders);
        info!(
            open_orders = state.open_orders.len(),
            bot_open_orders,
            external_open_orders,
            "execution reconciliation completed"
        );
        Ok(state.open_orders.values().cloned().collect())
    }

    async fn apply_execution_event(&self, event: &BinanceExecutionEvent) -> Result<ExecutionReport> {
        let client_order_id = event.report.client_order_id.clone();
        let observed_at = now_utc();
        let mut state = self.state.write().await;
        let mut correlated_report = event.report.clone();
        let mut first_report_latency = None;
        let mut first_fill_latency = None;

        let report_progressed = if let Some(tracked) = state.tracked_orders.get_mut(&client_order_id) {
            tracked.merge_event_fields(event);
            correlated_report = correlate_report_with_tracked(
                event.report.clone(),
                tracked,
                observed_at,
                &mut first_report_latency,
                &mut first_fill_latency,
            );
            tracked.update_from_report(&correlated_report)
        } else {
            correlated_report.exchange_order_age_ms = event
                .order_created_at
                .map(|created_at| (event.report.event_time - created_at).whole_milliseconds() as i64);
            true
        };

        let pending_cancel_kind = state.pending_cancel_reports.remove(&client_order_id);
        record_submit_to_first_report_latency(&mut state.aggregate, first_report_latency);
        record_submit_to_fill_latency(&mut state.aggregate, first_fill_latency);
        if report_progressed {
            update_aggregate_from_report(&mut state.aggregate, &correlated_report);
            if matches!(correlated_report.status, OrderStatus::Canceled) && pending_cancel_kind.is_none() {
                state.aggregate.total_canceled = state.aggregate.total_canceled.saturating_add(1);
                state.aggregate.total_external_cancels =
                    state.aggregate.total_external_cancels.saturating_add(1);
            }
        }

        match correlated_report.status {
            status if status.is_open() => {
                if let Some(kind) = pending_cancel_kind {
                    undo_pending_cancel(&mut state.aggregate, kind);
                }

                let tracked = state.tracked_orders.entry(client_order_id.clone()).or_insert_with(|| {
                    TrackedOrder::from_request(
                        build_request_from_event(event),
                        &correlated_report,
                        None,
                        Some(observed_at),
                    )
                });
                tracked.merge_event_fields(event);
                tracked.update_from_report(&correlated_report);

                if let Some(open_order) = tracked.to_open_order() {
                    state.open_orders.insert(client_order_id.clone(), open_order);
                }
            }
            OrderStatus::Canceled => {
                state.open_orders.remove(&client_order_id);
                state.tracked_orders.remove(&client_order_id);
            }
            _ => {
                if let Some(kind) = pending_cancel_kind {
                    undo_pending_cancel(&mut state.aggregate, kind);
                }
                state.open_orders.remove(&client_order_id);
                state.tracked_orders.remove(&client_order_id);
            }
        }

        info!(
            client_order_id = event.report.client_order_id,
            status = ?correlated_report.status,
            "execution state updated from user stream event"
        );
        Ok(correlated_report)
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
        intent_role: intent.role,
        exit_stage: intent.exit_stage,
        exit_reason: intent.exit_reason.clone(),
        edge_after_cost_bps: Some(intent.edge_after_cost_bps),
        expected_realized_edge_bps: Some(intent.expected_realized_edge_bps),
        adverse_selection_penalty_bps: Some(intent.adverse_selection_penalty_bps),
        source_intent_id: intent.intent_id.clone(),
    }
}

fn build_request_from_event(event: &BinanceExecutionEvent) -> OrderRequest {
    OrderRequest {
        client_order_id: event.report.client_order_id.clone(),
        symbol: event.report.symbol,
        side: event.side,
        order_type: event.order_type,
        price: event.price,
        quantity: event.original_quantity,
        time_in_force: event.time_in_force,
        post_only: matches!(event.order_type, OrderType::LimitMaker),
        reduce_only: false,
        intent_role: domain::IntentRole::AddRisk,
        exit_stage: None,
        exit_reason: None,
        edge_after_cost_bps: None,
        expected_realized_edge_bps: None,
        adverse_selection_penalty_bps: None,
        source_intent_id: "user-stream".to_string(),
    }
}

fn enrich_report(
    mut report: ExecutionReport,
    request: &OrderRequest,
    intent_created_at: common::Timestamp,
    submit_started_at: Option<common::Timestamp>,
    observed_at: common::Timestamp,
) -> ExecutionReport {
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
    report.decision_latency_ms = Some(
        submit_started_at
            .map(|started_at| (started_at - intent_created_at).whole_milliseconds() as i64)
            .unwrap_or_else(|| (observed_at - intent_created_at).whole_milliseconds() as i64),
    );
    report.submit_ack_latency_ms = submit_started_at
        .map(|started_at| (observed_at - started_at).whole_milliseconds() as i64);
    report.submit_to_first_report_ms = None;
    report.submit_to_fill_ms = if report.filled_quantity > Decimal::ZERO {
        report.submit_ack_latency_ms
    } else {
        None
    };
    report.edge_after_cost_bps = request.edge_after_cost_bps;
    report.expected_realized_edge_bps = request.expected_realized_edge_bps;
    report.adverse_selection_penalty_bps = request.adverse_selection_penalty_bps;
    report.intent_role = Some(request.intent_role);
    report.exit_stage = request.exit_stage;
    report.exit_reason = request.exit_reason.clone();
    report
}

fn upsert_state_from_request_report(
    state: &mut ExecutionState,
    request: OrderRequest,
    report: &ExecutionReport,
    submit_started_at: Option<common::Timestamp>,
    observed_at: Option<common::Timestamp>,
) {
    if report.status.is_open() {
        let tracked = TrackedOrder::from_request(request, report, submit_started_at, observed_at);
        if let Some(open_order) = tracked.to_open_order() {
            state
                .open_orders
                .insert(open_order.client_order_id.clone(), open_order);
        }
        state
            .tracked_orders
            .insert(report.client_order_id.clone(), tracked);
    } else {
        state.open_orders.remove(&report.client_order_id);
        state.tracked_orders.remove(&report.client_order_id);
    }
}

fn correlate_report_with_tracked(
    mut report: ExecutionReport,
    tracked: &mut TrackedOrder,
    observed_at: common::Timestamp,
    first_report_latency: &mut Option<i64>,
    first_fill_latency: &mut Option<i64>,
) -> ExecutionReport {
    report.decision_latency_ms = tracked.decision_latency_ms;
    report.submit_ack_latency_ms = tracked.submit_ack_latency_ms;
    report.edge_after_cost_bps = tracked.request.edge_after_cost_bps;
    report.expected_realized_edge_bps = tracked.request.expected_realized_edge_bps;
    report.adverse_selection_penalty_bps = tracked.request.adverse_selection_penalty_bps;
    report.intent_role = Some(tracked.request.intent_role);
    report.exit_stage = tracked.request.exit_stage;
    report.exit_reason = tracked.request.exit_reason.clone();

    if let Some(submit_started_at) = tracked.submit_started_at {
        let first_report_observed_at = tracked.first_report_observed_at.get_or_insert(observed_at);
        let report_latency_ms =
            (*first_report_observed_at - submit_started_at).whole_milliseconds() as i64;
        report.submit_to_first_report_ms = Some(report_latency_ms);
        if *first_report_observed_at == observed_at {
            *first_report_latency = Some(report_latency_ms);
        }

        if report.filled_quantity > Decimal::ZERO {
            let first_fill_observed_at = tracked.first_fill_observed_at.get_or_insert(observed_at);
            let latency_ms = (*first_fill_observed_at - submit_started_at).whole_milliseconds() as i64;
            report.submit_to_fill_ms = Some(latency_ms);
            if *first_fill_observed_at == observed_at {
                *first_fill_latency = Some(latency_ms);
            }
        } else {
            report.submit_to_fill_ms = tracked
                .first_fill_observed_at
                .map(|first_fill_observed_at| {
                    (first_fill_observed_at - submit_started_at).whole_milliseconds() as i64
                });
        }
    }

    report
}

fn find_equivalent_open_order(state: &ExecutionState, request: &OrderRequest) -> Option<String> {
    state
        .tracked_orders
        .iter()
        .find(|(client_order_id, tracked)| {
            is_bot_managed_client_order_id(client_order_id)
                && tracked.status.is_open()
                && equivalent_request(&tracked.request, request)
        })
        .map(|(client_order_id, _)| client_order_id.clone())
}

fn equivalent_request(left: &OrderRequest, right: &OrderRequest) -> bool {
    left.symbol == right.symbol
        && left.side == right.side
        && left.order_type == right.order_type
        && left.post_only == right.post_only
        && left.reduce_only == right.reduce_only
        && price_distance_bps(left.price, right.price) <= dec("4.5")
        && quantity_distance_ratio(left.quantity, right.quantity) <= dec("0.50")
}

fn quantity_distance_ratio(left: Decimal, right: Decimal) -> Decimal {
    if right.is_zero() {
        if left.is_zero() {
            Decimal::ZERO
        } else {
            Decimal::ONE
        }
    } else {
        ((left - right).abs() / right).abs()
    }
}

fn price_distance_bps(left: Option<Decimal>, right: Option<Decimal>) -> Decimal {
    match (left, right) {
        (Some(left), Some(right)) if right > Decimal::ZERO => {
            ((left - right).abs() / right) * Decimal::from(10_000u32)
        }
        (None, None) => Decimal::ZERO,
        _ => Decimal::from(10_000u32),
    }
}

fn evaluate_stale_order(
    order: &OpenOrder,
    max_age_ms: i64,
    now: common::Timestamp,
    mid_price: Option<Decimal>,
) -> StaleOrderEvaluation {
    let age_ms = (now - order.updated_at).whole_milliseconds() as i64;
    let age_budget_ms = if order.reduce_only {
        max_age_ms.saturating_mul(3) / 4
    } else {
        max_age_ms
    };
    let fill_ratio = if order.original_quantity.is_zero() {
        Decimal::ZERO
    } else {
        order.executed_quantity / order.original_quantity
    };
    let keep_distance_bps = if order.reduce_only && fill_ratio > Decimal::ZERO {
        dec("11.0")
    } else if order.reduce_only {
        dec("9.0")
    } else if fill_ratio > Decimal::ZERO {
        dec("14.0")
    } else {
        dec("12.0")
    };
    let hard_distance_bps = if order.reduce_only && fill_ratio > Decimal::ZERO {
        dec("24.0")
    } else if order.reduce_only {
        dec("20.0")
    } else if fill_ratio > Decimal::ZERO {
        dec("34.0")
    } else {
        dec("28.0")
    };

    let distance_bps = match (order.price, mid_price) {
        (Some(order_price), Some(mid_price)) if mid_price > Decimal::ZERO => {
            ((order_price - mid_price).abs() / mid_price) * Decimal::from(10_000u32)
        }
        _ => Decimal::ZERO,
    };

    let hard_stale = age_ms > max_age_ms.saturating_mul(3) || distance_bps > hard_distance_bps;
    let cancel_candidate = hard_stale
        || (age_ms > age_budget_ms
            && distance_bps
                > if order.reduce_only {
                    dec("5.5")
                } else {
                    dec("8.0")
                });
    let keep_worthy = age_ms <= age_budget_ms && distance_bps <= keep_distance_bps;

    StaleOrderEvaluation {
        cancel_candidate,
        keep_worthy,
        hard_stale,
    }
}

fn should_cancel_order(evaluation: StaleOrderEvaluation) -> bool {
    evaluation.hard_stale || (evaluation.cancel_candidate && !evaluation.keep_worthy)
}

fn mark_order_canceled(
    state: &mut ExecutionState,
    client_order_id: &str,
    cancel_kind: PendingCancelKind,
) -> bool {
    if !state.open_orders.contains_key(client_order_id) {
        return false;
    }

    state
        .pending_cancel_reports
        .insert(client_order_id.to_string(), cancel_kind);

    state.open_orders.remove(client_order_id);
    state.tracked_orders.remove(client_order_id);
    state.aggregate.total_canceled = state.aggregate.total_canceled.saturating_add(1);
    match cancel_kind {
        PendingCancelKind::Manual => {
            state.aggregate.total_manual_cancels =
                state.aggregate.total_manual_cancels.saturating_add(1);
        }
        PendingCancelKind::Stale => {
            state.aggregate.total_stale_cancels =
                state.aggregate.total_stale_cancels.saturating_add(1);
        }
    }

    true
}

fn undo_pending_cancel(aggregate: &mut ExecutionAggregate, cancel_kind: PendingCancelKind) {
    aggregate.total_canceled = aggregate.total_canceled.saturating_sub(1);
    match cancel_kind {
        PendingCancelKind::Manual => {
            aggregate.total_manual_cancels = aggregate.total_manual_cancels.saturating_sub(1);
        }
        PendingCancelKind::Stale => {
            aggregate.total_stale_cancels = aggregate.total_stale_cancels.saturating_sub(1);
        }
    }
}

fn update_aggregate_from_report(aggregate: &mut ExecutionAggregate, report: &ExecutionReport) {
    if !is_bot_managed_client_order_id(&report.client_order_id) {
        return;
    }

    if matches!(report.status, OrderStatus::Rejected) {
        if is_local_validation_reject(report) {
            aggregate.total_local_validation_rejects =
                aggregate.total_local_validation_rejects.saturating_add(1);
        } else {
            aggregate.total_rejected = aggregate.total_rejected.saturating_add(1);
            if is_benign_exchange_reject(report) {
                aggregate.total_benign_exchange_rejected =
                    aggregate.total_benign_exchange_rejected.saturating_add(1);
            } else {
                aggregate.risk_scored_rejections =
                    aggregate.risk_scored_rejections.saturating_add(1);
            }
        }
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
}

fn record_decision_latency(aggregate: &mut ExecutionAggregate, latency_ms: Option<i64>) {
    if let Some(latency_ms) = latency_ms {
        aggregate.decision_latency_ms_sum += Decimal::from(latency_ms);
        aggregate.decision_latency_samples = aggregate.decision_latency_samples.saturating_add(1);
    }
}

fn record_submit_ack_latency(aggregate: &mut ExecutionAggregate, latency_ms: Option<i64>) {
    if let Some(latency_ms) = latency_ms {
        aggregate.submit_ack_latency_ms_sum += Decimal::from(latency_ms);
        aggregate.submit_ack_latency_samples = aggregate.submit_ack_latency_samples.saturating_add(1);
    }
}

fn record_submit_to_first_report_latency(
    aggregate: &mut ExecutionAggregate,
    latency_ms: Option<i64>,
) {
    if let Some(latency_ms) = latency_ms {
        aggregate.submit_to_first_report_ms_sum += Decimal::from(latency_ms);
        aggregate.submit_to_first_report_samples =
            aggregate.submit_to_first_report_samples.saturating_add(1);
    }
}

fn record_submit_to_fill_latency(aggregate: &mut ExecutionAggregate, latency_ms: Option<i64>) {
    if let Some(latency_ms) = latency_ms {
        aggregate.submit_to_fill_ms_sum += Decimal::from(latency_ms);
        aggregate.submit_to_fill_samples = aggregate.submit_to_fill_samples.saturating_add(1);
    }
}

fn record_cancel_ack_latency(aggregate: &mut ExecutionAggregate, latency_ms: Option<i64>) {
    if let Some(latency_ms) = latency_ms {
        aggregate.cancel_ack_latency_ms_sum += Decimal::from(latency_ms);
        aggregate.cancel_ack_latency_samples = aggregate.cancel_ack_latency_samples.saturating_add(1);
    }
}

fn build_stats(aggregate: &ExecutionAggregate) -> ExecutionStats {
    let updated_at = now_utc();
    ExecutionStats {
        total_submitted: aggregate.total_submitted,
        total_rejected: aggregate.total_rejected,
        total_local_validation_rejects: aggregate.total_local_validation_rejects,
        total_benign_exchange_rejected: aggregate.total_benign_exchange_rejected,
        risk_scored_rejections: aggregate.risk_scored_rejections,
        total_canceled: aggregate.total_canceled,
        total_manual_cancels: aggregate.total_manual_cancels,
        total_external_cancels: aggregate.total_external_cancels,
        total_filled_reports: aggregate.total_filled_reports,
        total_stale_cancels: aggregate.total_stale_cancels,
        total_duplicate_intents: aggregate.total_duplicate_intents,
        total_equivalent_order_skips: aggregate.total_equivalent_order_skips,
        reject_rate: if aggregate.total_submitted == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(aggregate.total_rejected) / Decimal::from(aggregate.total_submitted)
        },
        risk_reject_rate: if aggregate.total_submitted == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(aggregate.risk_scored_rejections) / Decimal::from(aggregate.total_submitted)
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
        avg_submit_ack_latency_ms: if aggregate.submit_ack_latency_samples == 0 {
            Decimal::ZERO
        } else {
            aggregate.submit_ack_latency_ms_sum / Decimal::from(aggregate.submit_ack_latency_samples)
        },
        avg_submit_to_first_report_ms: if aggregate.submit_to_first_report_samples == 0 {
            Decimal::ZERO
        } else {
            aggregate.submit_to_first_report_ms_sum
                / Decimal::from(aggregate.submit_to_first_report_samples)
        },
        avg_submit_to_fill_ms: if aggregate.submit_to_fill_samples == 0 {
            Decimal::ZERO
        } else {
            aggregate.submit_to_fill_ms_sum / Decimal::from(aggregate.submit_to_fill_samples)
        },
        avg_cancel_ack_latency_ms: if aggregate.cancel_ack_latency_samples == 0 {
            Decimal::ZERO
        } else {
            aggregate.cancel_ack_latency_ms_sum / Decimal::from(aggregate.cancel_ack_latency_samples)
        },
        decision_latency_samples: aggregate.decision_latency_samples,
        submit_ack_latency_samples: aggregate.submit_ack_latency_samples,
        submit_to_first_report_samples: aggregate.submit_to_first_report_samples,
        submit_to_fill_samples: aggregate.submit_to_fill_samples,
        cancel_ack_latency_samples: aggregate.cancel_ack_latency_samples,
        updated_at,
    }
}

fn is_bot_managed_client_order_id(client_order_id: &str) -> bool {
    client_order_id.starts_with("bot-")
}

fn is_local_validation_reject(report: &ExecutionReport) -> bool {
    report
        .message
        .as_deref()
        .map(|message| message.to_ascii_lowercase().starts_with("local order rejection:"))
        .unwrap_or(false)
}

fn is_benign_exchange_reject(report: &ExecutionReport) -> bool {
    let Some(message) = report.message.as_deref() else {
        return false;
    };
    let normalized = message.to_ascii_lowercase();
    normalized.contains("immediately match and take")
        || normalized.contains("would match and take")
        || normalized.contains("post only")
}

#[allow(dead_code)]
fn _rejected_report(request: &OrderRequest) -> ExecutionReport {
    ExecutionReport {
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: None,
        symbol: request.symbol,
        status: OrderStatus::Rejected,
        filled_quantity: Decimal::ZERO,
        average_fill_price: None,
        fill_ratio: Decimal::ZERO,
        requested_price: request.price,
        slippage_bps: None,
        decision_latency_ms: None,
        submit_ack_latency_ms: None,
        submit_to_first_report_ms: None,
        submit_to_fill_ms: None,
        exchange_order_age_ms: None,
        edge_after_cost_bps: request.edge_after_cost_bps,
        expected_realized_edge_bps: request.expected_realized_edge_bps,
        adverse_selection_penalty_bps: request.adverse_selection_penalty_bps,
        intent_role: Some(request.intent_role),
        exit_stage: request.exit_stage,
        exit_reason: request.exit_reason.clone(),
        message: Some("execution rejection".to_string()),
        event_time: now_utc(),
    }
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use domain::{
        AccountSnapshot, Balance, FillEvent, MarketTrade, PriceLevel, RiskAction, Side,
        StrategyKind,
    };
    use exchange_binance_spot::{
        BinanceBootstrapState, BinanceMarketEvent, BinanceSpotGateway, BinanceUserStreamEvent,
        MarketStreamHandle, UserStreamHandle,
    };
    use tokio::sync::{mpsc, watch};

    #[derive(Debug)]
    struct TestGatewayState {
        clock_time: common::Timestamp,
        placed_orders: Vec<OrderRequest>,
        canceled_orders: Vec<String>,
        fetched_open_orders: HashMap<Symbol, Vec<OpenOrder>>,
        prepared_order: Option<PreparedOrder>,
    }

    impl Default for TestGatewayState {
        fn default() -> Self {
            Self {
                clock_time: now_utc(),
                placed_orders: Vec::new(),
                canceled_orders: Vec::new(),
                fetched_open_orders: HashMap::new(),
                prepared_order: None,
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestGateway {
        state: Arc<RwLock<TestGatewayState>>,
    }

    impl TestGateway {
        fn new() -> Self {
            Self {
                state: Arc::new(RwLock::new(TestGatewayState::default())),
            }
        }

        async fn placed_orders(&self) -> Vec<OrderRequest> {
            self.state.read().await.placed_orders.clone()
        }

        async fn canceled_orders(&self) -> Vec<String> {
            self.state.read().await.canceled_orders.clone()
        }

        async fn set_prepared_order(&self, prepared_order: PreparedOrder) {
            self.state.write().await.prepared_order = Some(prepared_order);
        }
    }

    #[async_trait]
    impl BinanceSpotGateway for TestGateway {
        async fn sync_clock(&self) -> Result<common::Timestamp> {
            Ok(self.state.read().await.clock_time)
        }

        async fn ping_rest(&self) -> Result<()> {
            Ok(())
        }

        async fn fetch_account_snapshot(&self) -> Result<AccountSnapshot> {
            Ok(AccountSnapshot {
                balances: vec![Balance {
                    asset: "USDC".to_string(),
                    free: dec("1000"),
                    locked: Decimal::ZERO,
                }],
                updated_at: now_utc(),
            })
        }

        async fn fetch_open_orders(&self, symbol: Symbol) -> Result<Vec<OpenOrder>> {
            Ok(self
                .state
                .read()
                .await
                .fetched_open_orders
                .get(&symbol)
                .cloned()
                .unwrap_or_default())
        }

        async fn fetch_recent_fills(&self, _symbol: Symbol, _limit: usize) -> Result<Vec<FillEvent>> {
            Ok(Vec::new())
        }

        async fn fetch_orderbook_snapshot(&self, symbol: Symbol, _depth: usize) -> Result<domain::OrderBookSnapshot> {
            Ok(domain::OrderBookSnapshot {
                symbol,
                bids: vec![PriceLevel {
                    price: dec("100"),
                    quantity: dec("1"),
                }],
                asks: vec![PriceLevel {
                    price: dec("100.02"),
                    quantity: dec("1"),
                }],
                last_update_id: 1,
                exchange_time: Some(now_utc()),
                observed_at: now_utc(),
            })
        }

        async fn fetch_recent_trades(&self, symbol: Symbol, _limit: usize) -> Result<Vec<MarketTrade>> {
            Ok(vec![MarketTrade {
                symbol,
                trade_id: "trade-1".to_string(),
                price: dec("100"),
                quantity: dec("0.1"),
                aggressor_side: Side::Buy,
                event_time: now_utc(),
                received_at: now_utc(),
            }])
        }

        async fn fetch_bootstrap_state(&self, symbols: &[Symbol]) -> Result<BinanceBootstrapState> {
            let mut open_orders = Vec::new();
            for &symbol in symbols {
                open_orders.extend(self.fetch_open_orders(symbol).await?);
            }

            Ok(BinanceBootstrapState {
                account: self.fetch_account_snapshot().await?,
                open_orders,
                fills: Vec::new(),
                fetched_at: now_utc(),
            })
        }

        async fn poll_market_events(&self, _symbols: &[Symbol]) -> Result<Vec<BinanceMarketEvent>> {
            Ok(Vec::new())
        }

        async fn poll_account_events(&self, _symbols: &[Symbol]) -> Result<Vec<BinanceUserStreamEvent>> {
            Ok(Vec::new())
        }

        async fn prepare_order(&self, request: OrderRequest) -> Result<PreparedOrder> {
            Ok(self
                .state
                .read()
                .await
                .prepared_order
                .clone()
                .unwrap_or(PreparedOrder::Ready(request)))
        }

        async fn place_order(&self, request: OrderRequest) -> Result<ExecutionReport> {
            let mut state = self.state.write().await;
            state.placed_orders.push(request.clone());
            let status = if matches!(request.order_type, OrderType::Market) {
                OrderStatus::Filled
            } else {
                OrderStatus::New
            };

            Ok(ExecutionReport {
                client_order_id: request.client_order_id.clone(),
                exchange_order_id: Some(format!("mock-{}", state.placed_orders.len())),
                symbol: request.symbol,
                status,
                filled_quantity: if matches!(status, OrderStatus::Filled) {
                    request.quantity
                } else {
                    Decimal::ZERO
                },
                average_fill_price: request.price,
                fill_ratio: Decimal::ZERO,
                requested_price: request.price,
                slippage_bps: None,
                decision_latency_ms: Some(0),
                submit_ack_latency_ms: Some(0),
                submit_to_first_report_ms: None,
                submit_to_fill_ms: if matches!(status, OrderStatus::Filled) {
                    Some(0)
                } else {
                    None
                },
                exchange_order_age_ms: None,
                intent_role: Some(request.intent_role),
                exit_stage: request.exit_stage,
                exit_reason: request.exit_reason.clone(),
                message: Some("mock accepted".to_string()),
                event_time: state.clock_time,
            })
        }

        async fn cancel_all_orders(&self, symbol: Symbol) -> Result<()> {
            let mut state = self.state.write().await;
            state.fetched_open_orders.remove(&symbol);
            Ok(())
        }

        async fn cancel_order(&self, order: &OpenOrder) -> Result<()> {
            let mut state = self.state.write().await;
            state.canceled_orders.push(order.client_order_id.clone());
            if let Some(orders) = state.fetched_open_orders.get_mut(&order.symbol) {
                orders.retain(|candidate| candidate.client_order_id != order.client_order_id);
            }
            Ok(())
        }

        async fn open_market_stream(&self, _symbols: &[Symbol]) -> Result<MarketStreamHandle> {
            let (_event_tx, event_rx) = mpsc::channel(1);
            let (_status_tx, status_rx) = mpsc::channel(1);
            let (shutdown_tx, _shutdown_rx) = watch::channel(false);
            Ok(MarketStreamHandle {
                events: event_rx,
                status: status_rx,
                shutdown: shutdown_tx,
            })
        }

        async fn open_user_stream(&self) -> Result<UserStreamHandle> {
            let (_event_tx, event_rx) = mpsc::channel(1);
            let (_status_tx, status_rx) = mpsc::channel(1);
            let (shutdown_tx, _shutdown_rx) = watch::channel(false);
            Ok(UserStreamHandle {
                events: event_rx,
                status: status_rx,
                shutdown: shutdown_tx,
            })
        }
    }

    fn sample_intent(intent_id: &str, symbol: Symbol, side: Side, price: Decimal, quantity: Decimal) -> TradeIntent {
        TradeIntent {
            intent_id: intent_id.to_string(),
            symbol,
            strategy: StrategyKind::MarketMaking,
            side,
            quantity,
            limit_price: Some(price),
            max_slippage_bps: dec("2"),
            post_only: true,
            reduce_only: false,
            time_in_force: Some(TimeInForce::Gtc),
            role: domain::IntentRole::AddRisk,
            exit_stage: None,
            exit_reason: None,
            expected_edge_bps: dec("3"),
            expected_fee_bps: dec("0.75"),
            expected_slippage_bps: Decimal::ZERO,
            edge_after_cost_bps: dec("2"),
            reason: "test intent".to_string(),
            created_at: now_utc(),
            expires_at: None,
        }
    }

    fn sample_decision(intent: TradeIntent) -> RiskDecision {
        RiskDecision {
            action: RiskAction::Approve,
            original_intent: intent.clone(),
            approved_intent: Some(intent),
            reason: "approved".to_string(),
            resulting_mode: RiskMode::Normal,
            decided_at: now_utc(),
        }
    }

    fn sample_open_order(
        client_order_id: &str,
        symbol: Symbol,
        side: Side,
        price: Decimal,
        age_ms: i64,
    ) -> OpenOrder {
        OpenOrder {
            client_order_id: client_order_id.to_string(),
            exchange_order_id: Some(format!("ex-{client_order_id}")),
            symbol,
            side,
            price: Some(price),
            original_quantity: dec("0.01"),
            executed_quantity: Decimal::ZERO,
            status: OrderStatus::New,
            reduce_only: false,
            intent_role: domain::IntentRole::AddRisk,
            exit_stage: None,
            exit_reason: None,
            updated_at: timestamp_minus_ms(age_ms),
        }
    }

    fn execution_event_from_request(
        request: &OrderRequest,
        status: OrderStatus,
        cumulative_filled_quantity: Decimal,
    ) -> BinanceExecutionEvent {
        let price = request.price.unwrap_or(dec("100"));
        BinanceExecutionEvent {
            report: ExecutionReport {
                client_order_id: request.client_order_id.clone(),
                exchange_order_id: Some("exchange-1".to_string()),
                symbol: request.symbol,
                status,
                filled_quantity: cumulative_filled_quantity,
                average_fill_price: Some(price),
                fill_ratio: if request.quantity.is_zero() {
                    Decimal::ZERO
                } else {
                    cumulative_filled_quantity / request.quantity
                },
                requested_price: request.price,
                slippage_bps: None,
                decision_latency_ms: None,
                submit_ack_latency_ms: None,
                submit_to_first_report_ms: None,
                submit_to_fill_ms: None,
                exchange_order_age_ms: None,
                intent_role: Some(request.intent_role),
                exit_stage: request.exit_stage,
                exit_reason: request.exit_reason.clone(),
                message: Some("event".to_string()),
                event_time: now_utc(),
            },
            side: request.side,
            order_type: request.order_type,
            time_in_force: request.time_in_force,
            original_quantity: request.quantity,
            price: request.price,
            cumulative_filled_quantity,
            cumulative_quote_quantity: cumulative_filled_quantity * price,
            last_executed_quantity: cumulative_filled_quantity,
            last_executed_price: Some(price),
            reject_reason: None,
            is_working: status.is_open(),
            is_maker: matches!(request.order_type, OrderType::LimitMaker),
            order_created_at: Some(now_utc()),
            transaction_time: now_utc(),
            fill: None,
            fill_recovery: None,
        }
    }

    fn timestamp_minus_ms(age_ms: i64) -> common::Timestamp {
        common::Timestamp::from_unix_timestamp_nanos(
            now_utc().unix_timestamp_nanos() - (age_ms as i128 * 1_000_000),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn stale_off_market_order_is_canceled() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        engine
            .sync_open_orders(vec![sample_open_order(
                "bot-btc-stale",
                Symbol::BtcUsdc,
                Side::Buy,
                dec("90"),
                6_000,
            )])
            .await
            .unwrap();

        let canceled = engine
            .cancel_stale_orders(5_000, &HashMap::from([(Symbol::BtcUsdc, dec("100"))]))
            .await
            .unwrap();

        assert_eq!(canceled, 1);
        assert_eq!(gateway.canceled_orders().await, vec!["bot-btc-stale".to_string()]);
        assert!(engine.open_orders().await.is_empty());
    }

    #[tokio::test]
    async fn close_valid_order_is_not_canceled() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        engine
            .sync_open_orders(vec![sample_open_order(
                "bot-btc-keep",
                Symbol::BtcUsdc,
                Side::Buy,
                dec("99.98"),
                1_000,
            )])
            .await
            .unwrap();

        let canceled = engine
            .cancel_stale_orders(5_000, &HashMap::from([(Symbol::BtcUsdc, dec("100"))]))
            .await
            .unwrap();

        assert_eq!(canceled, 0);
        assert!(gateway.canceled_orders().await.is_empty());
        assert_eq!(engine.open_orders().await.len(), 1);
    }

    #[tokio::test]
    async fn mixed_orders_cancel_only_truly_stale_orders() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc, Symbol::EthUsdc]);

        engine
            .sync_open_orders(vec![
                sample_open_order("bot-btc-stale", Symbol::BtcUsdc, Side::Buy, dec("88"), 7_000),
                sample_open_order("bot-btc-keep", Symbol::BtcUsdc, Side::Sell, dec("100.03"), 900),
                sample_open_order("bot-eth-stale", Symbol::EthUsdc, Side::Buy, dec("1500"), 7_000),
            ])
            .await
            .unwrap();

        let canceled = engine
            .cancel_stale_orders(
                5_000,
                &HashMap::from([(Symbol::BtcUsdc, dec("100")), (Symbol::EthUsdc, dec("2000"))]),
            )
            .await
            .unwrap();

        assert_eq!(canceled, 2);
        let mut canceled_orders = gateway.canceled_orders().await;
        canceled_orders.sort();
        assert_eq!(
            canceled_orders,
            vec!["bot-btc-stale".to_string(), "bot-eth-stale".to_string()]
        );
        let remaining = engine.open_orders().await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].client_order_id, "bot-btc-keep");
    }

    #[tokio::test]
    async fn tracking_stays_coherent_after_report_cancel_and_fill() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        let first_intent = sample_intent("intent-1", Symbol::BtcUsdc, Side::Buy, dec("100"), dec("0.010"));
        let first_report = engine.execute(sample_decision(first_intent)).await.unwrap().unwrap();
        let first_request = gateway.placed_orders().await[0].clone();

        let partial_event =
            execution_event_from_request(&first_request, OrderStatus::PartiallyFilled, dec("0.004"));
        engine.apply_execution_event(&partial_event).await.unwrap();

        let open_orders = engine.open_orders().await;
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].executed_quantity, dec("0.004"));

        let cancel_event =
            execution_event_from_request(&first_request, OrderStatus::Canceled, dec("0.004"));
        engine.apply_execution_event(&cancel_event).await.unwrap();

        assert!(engine.open_orders().await.is_empty());
        {
            let state = engine.state.read().await;
            assert!(!state.tracked_orders.contains_key(&first_report.client_order_id));
        }

        let second_intent = sample_intent("intent-2", Symbol::BtcUsdc, Side::Sell, dec("100.05"), dec("0.010"));
        let second_report = engine.execute(sample_decision(second_intent)).await.unwrap().unwrap();
        let second_request = gateway.placed_orders().await[1].clone();
        let fill_event = execution_event_from_request(&second_request, OrderStatus::Filled, dec("0.010"));
        engine.apply_execution_event(&fill_event).await.unwrap();

        assert!(engine.open_orders().await.is_empty());
        let state = engine.state.read().await;
        assert!(!state.tracked_orders.contains_key(&second_report.client_order_id));
    }

    #[tokio::test]
    async fn normal_execution_places_order_and_skips_equivalent_duplicate() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        let first_intent = sample_intent("intent-1", Symbol::BtcUsdc, Side::Buy, dec("100"), dec("0.010"));
        let first_report = engine.execute(sample_decision(first_intent)).await.unwrap();
        assert!(first_report.is_some());

        let duplicate_intent = sample_intent("intent-2", Symbol::BtcUsdc, Side::Buy, dec("100.005"), dec("0.0105"));
        let second_report = engine.execute(sample_decision(duplicate_intent)).await.unwrap();

        assert!(second_report.is_none());
        assert_eq!(gateway.placed_orders().await.len(), 1);
        assert_eq!(engine.open_orders().await.len(), 1);
        let stats = engine.stats().await;
        assert_eq!(stats.total_equivalent_order_skips, 1);
        assert_eq!(stats.total_duplicate_intents, 0);
    }

    #[tokio::test]
    async fn prepared_order_is_the_one_sent_to_exchange_and_tracked() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);
        let mut normalized_request =
            build_order_request(&sample_intent("intent-1", Symbol::BtcUsdc, Side::Buy, dec("100"), dec("0.010")));
        normalized_request.price = Some(dec("99.99"));
        normalized_request.quantity = dec("0.009");
        gateway
            .set_prepared_order(PreparedOrder::Ready(normalized_request.clone()))
            .await;

        let report = engine
            .execute(sample_decision(sample_intent(
                "intent-1",
                Symbol::BtcUsdc,
                Side::Buy,
                dec("100"),
                dec("0.010"),
            )))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(gateway.placed_orders().await, vec![normalized_request.clone()]);
        let open_orders = engine.open_orders().await;
        assert_eq!(open_orders.len(), 1);
        assert_eq!(open_orders[0].price, normalized_request.price);
        assert_eq!(open_orders[0].original_quantity, normalized_request.quantity);
        assert_eq!(report.requested_price, normalized_request.price);
    }

    #[tokio::test]
    async fn local_prepare_rejection_returns_report_without_exchange_submit() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);
        let rejected_request =
            build_order_request(&sample_intent("intent-1", Symbol::BtcUsdc, Side::Buy, dec("100"), dec("0.010")));
        gateway
            .set_prepared_order(PreparedOrder::Rejected(ExecutionReport {
                client_order_id: rejected_request.client_order_id.clone(),
                exchange_order_id: None,
                symbol: rejected_request.symbol,
                status: OrderStatus::Rejected,
                filled_quantity: Decimal::ZERO,
                average_fill_price: None,
                fill_ratio: Decimal::ZERO,
                requested_price: rejected_request.price,
                slippage_bps: None,
                decision_latency_ms: None,
                submit_ack_latency_ms: None,
                submit_to_first_report_ms: None,
                submit_to_fill_ms: None,
                exchange_order_age_ms: None,
                intent_role: Some(rejected_request.intent_role),
                exit_stage: rejected_request.exit_stage,
                exit_reason: rejected_request.exit_reason.clone(),
                message: Some("local order rejection: invalid LOT_SIZE".to_string()),
                event_time: now_utc(),
            }))
            .await;

        let report = engine
            .execute(sample_decision(sample_intent(
                "intent-1",
                Symbol::BtcUsdc,
                Side::Buy,
                dec("100"),
                dec("0.010"),
            )))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(report.status, OrderStatus::Rejected);
        assert!(report
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("local order rejection"));
        assert!(gateway.placed_orders().await.is_empty());
        let stats = engine.stats().await;
        assert_eq!(stats.total_submitted, 0);
        assert_eq!(stats.total_rejected, 0);
        assert_eq!(stats.total_local_validation_rejects, 1);
        assert_eq!(stats.decision_latency_samples, 1);
        assert_eq!(stats.submit_ack_latency_samples, 0);
        assert_eq!(stats.submit_to_first_report_samples, 0);
        assert_eq!(stats.submit_to_fill_samples, 0);
    }

    #[tokio::test]
    async fn first_user_stream_report_latency_is_separate_from_submit_ack_latency() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        let report = engine
            .execute(sample_decision(sample_intent(
                "intent-1",
                Symbol::BtcUsdc,
                Side::Buy,
                dec("100"),
                dec("0.010"),
            )))
            .await
            .unwrap()
            .unwrap();
        assert!(report.submit_ack_latency_ms.is_some());
        assert!(report.submit_to_first_report_ms.is_none());

        let request = gateway.placed_orders().await[0].clone();
        let mut event = execution_event_from_request(&request, OrderStatus::New, Decimal::ZERO);
        event.report.exchange_order_id = report.exchange_order_id.clone();
        let correlated = engine.apply_execution_event(&event).await.unwrap();

        assert_eq!(correlated.decision_latency_ms, report.decision_latency_ms);
        assert_eq!(correlated.submit_ack_latency_ms, report.submit_ack_latency_ms);
        assert!(correlated.submit_to_first_report_ms.is_some());

        let stats = engine.stats().await;
        assert_eq!(stats.submit_ack_latency_samples, 1);
        assert_eq!(stats.submit_to_first_report_samples, 1);
    }

    #[tokio::test]
    async fn external_execution_events_do_not_pollute_bot_latency_stats() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        let event = BinanceExecutionEvent {
            report: ExecutionReport {
                client_order_id: "manual-1".to_string(),
                exchange_order_id: Some("manual-exchange-1".to_string()),
                symbol: Symbol::BtcUsdc,
                status: OrderStatus::New,
                filled_quantity: Decimal::ZERO,
                average_fill_price: Some(dec("100")),
                fill_ratio: Decimal::ZERO,
                requested_price: Some(dec("100")),
                slippage_bps: None,
                decision_latency_ms: None,
                submit_ack_latency_ms: None,
                submit_to_first_report_ms: None,
                submit_to_fill_ms: None,
                exchange_order_age_ms: Some(8_000),
                intent_role: None,
                exit_stage: None,
                exit_reason: None,
                message: Some("manual".to_string()),
                event_time: now_utc(),
            },
            side: Side::Buy,
            order_type: OrderType::LimitMaker,
            time_in_force: Some(TimeInForce::Gtc),
            original_quantity: dec("0.01"),
            price: Some(dec("100")),
            cumulative_filled_quantity: Decimal::ZERO,
            cumulative_quote_quantity: Decimal::ZERO,
            last_executed_quantity: Decimal::ZERO,
            last_executed_price: Some(dec("100")),
            reject_reason: None,
            is_working: true,
            is_maker: true,
            order_created_at: Some(timestamp_minus_ms(8_000)),
            transaction_time: now_utc(),
            fill: None,
            fill_recovery: None,
        };

        let correlated = engine.apply_execution_event(&event).await.unwrap();
        let exchange_order_age_ms = correlated.exchange_order_age_ms.unwrap_or_default();
        assert!(exchange_order_age_ms >= 7_900);
        assert!(exchange_order_age_ms <= 8_100);

        let stats = engine.stats().await;
        assert_eq!(stats.total_submitted, 0);
        assert_eq!(stats.decision_latency_samples, 0);
        assert_eq!(stats.submit_ack_latency_samples, 0);
        assert_eq!(stats.submit_to_first_report_samples, 0);
        assert_eq!(stats.submit_to_fill_samples, 0);
    }

    #[tokio::test]
    async fn cancel_all_tracks_cancel_ack_latency_without_execution_reports() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        engine
            .sync_open_orders(vec![sample_open_order(
                "bot-btc-cancel",
                Symbol::BtcUsdc,
                Side::Buy,
                dec("100"),
                1_000,
            )])
            .await
            .unwrap();

        engine.cancel_all(Some(Symbol::BtcUsdc)).await.unwrap();

        let stats = engine.stats().await;
        assert_eq!(stats.cancel_ack_latency_samples, 1);
        assert_eq!(stats.submit_ack_latency_samples, 0);
        assert_eq!(stats.submit_to_fill_samples, 0);
    }

    #[tokio::test]
    async fn execution_stats_capture_duplicate_intents_and_manual_cancels() {
        let gateway = TestGateway::new();
        let engine = BinanceExecutionEngine::new(gateway.clone(), vec![Symbol::BtcUsdc]);

        let first_intent = sample_intent("intent-1", Symbol::BtcUsdc, Side::Buy, dec("100"), dec("0.010"));
        let first_decision = sample_decision(first_intent.clone());
        let first_report = engine.execute(first_decision.clone()).await.unwrap();
        assert!(first_report.is_some());

        let duplicate_report = engine.execute(first_decision).await.unwrap();
        assert!(duplicate_report.is_none());

        engine.cancel_all(Some(Symbol::BtcUsdc)).await.unwrap();

        let stats = engine.stats().await;
        assert_eq!(stats.total_duplicate_intents, 1);
        assert_eq!(stats.total_manual_cancels, 1);
    }
}
