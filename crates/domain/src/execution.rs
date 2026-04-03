use crate::{ExitStage, IntentRole, LiquiditySide, Side, Symbol, TimeInForce};
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Limit,
    Market,
    LimitMaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

impl OrderStatus {
    pub const fn is_open(self) -> bool {
        matches!(self, Self::New | Self::PartiallyFilled)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub client_order_id: String,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub time_in_force: Option<TimeInForce>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub intent_role: IntentRole,
    pub exit_stage: Option<ExitStage>,
    pub exit_reason: Option<String>,
    pub edge_after_cost_bps: Option<Decimal>,
    pub expected_realized_edge_bps: Option<Decimal>,
    pub adverse_selection_penalty_bps: Option<Decimal>,
    pub source_intent_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrder {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Option<Decimal>,
    pub original_quantity: Decimal,
    pub executed_quantity: Decimal,
    pub status: OrderStatus,
    pub reduce_only: bool,
    pub intent_role: IntentRole,
    pub exit_stage: Option<ExitStage>,
    pub exit_reason: Option<String>,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub symbol: Symbol,
    pub status: OrderStatus,
    pub filled_quantity: Decimal,
    pub average_fill_price: Option<Decimal>,
    pub fill_ratio: Decimal,
    pub requested_price: Option<Decimal>,
    pub slippage_bps: Option<Decimal>,
    pub decision_latency_ms: Option<i64>,
    pub submit_ack_latency_ms: Option<i64>,
    pub submit_to_first_report_ms: Option<i64>,
    pub submit_to_fill_ms: Option<i64>,
    pub exchange_order_age_ms: Option<i64>,
    pub edge_after_cost_bps: Option<Decimal>,
    pub expected_realized_edge_bps: Option<Decimal>,
    pub adverse_selection_penalty_bps: Option<Decimal>,
    pub intent_role: Option<IntentRole>,
    pub exit_stage: Option<ExitStage>,
    pub exit_reason: Option<String>,
    pub message: Option<String>,
    pub event_time: Timestamp,
}

impl ExecutionReport {
    pub fn to_open_order(&self, request: &OrderRequest) -> Option<OpenOrder> {
        self.status.is_open().then(|| OpenOrder {
            client_order_id: self.client_order_id.clone(),
            exchange_order_id: self.exchange_order_id.clone(),
            symbol: self.symbol,
            side: request.side,
            price: request.price,
            original_quantity: request.quantity,
            executed_quantity: self.filled_quantity,
            status: self.status,
            reduce_only: request.reduce_only,
            intent_role: request.intent_role,
            exit_stage: request.exit_stage,
            exit_reason: request.exit_reason.clone(),
            updated_at: self.event_time,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub total_submitted: u64,
    pub total_rejected: u64,
    pub total_local_validation_rejects: u64,
    pub total_benign_exchange_rejected: u64,
    pub risk_scored_rejections: u64,
    pub total_canceled: u64,
    pub total_manual_cancels: u64,
    pub total_external_cancels: u64,
    pub total_filled_reports: u64,
    pub total_stale_cancels: u64,
    pub total_duplicate_intents: u64,
    pub total_equivalent_order_skips: u64,
    pub reject_rate: Decimal,
    pub risk_reject_rate: Decimal,
    pub avg_fill_ratio: Decimal,
    pub avg_slippage_bps: Decimal,
    pub avg_decision_latency_ms: Decimal,
    pub avg_submit_ack_latency_ms: Decimal,
    pub avg_submit_to_first_report_ms: Decimal,
    pub avg_submit_to_fill_ms: Decimal,
    pub avg_cancel_ack_latency_ms: Decimal,
    pub decision_latency_samples: u64,
    pub submit_ack_latency_samples: u64,
    pub submit_to_first_report_samples: u64,
    pub submit_to_fill_samples: u64,
    pub cancel_ack_latency_samples: u64,
    pub updated_at: Timestamp,
}

impl ExecutionStats {
    pub fn empty(updated_at: Timestamp) -> Self {
        Self {
            total_submitted: 0,
            total_rejected: 0,
            total_local_validation_rejects: 0,
            total_benign_exchange_rejected: 0,
            risk_scored_rejections: 0,
            total_canceled: 0,
            total_manual_cancels: 0,
            total_external_cancels: 0,
            total_filled_reports: 0,
            total_stale_cancels: 0,
            total_duplicate_intents: 0,
            total_equivalent_order_skips: 0,
            reject_rate: Decimal::ZERO,
            risk_reject_rate: Decimal::ZERO,
            avg_fill_ratio: Decimal::ZERO,
            avg_slippage_bps: Decimal::ZERO,
            avg_decision_latency_ms: Decimal::ZERO,
            avg_submit_ack_latency_ms: Decimal::ZERO,
            avg_submit_to_first_report_ms: Decimal::ZERO,
            avg_submit_to_fill_ms: Decimal::ZERO,
            avg_cancel_ack_latency_ms: Decimal::ZERO,
            decision_latency_samples: 0,
            submit_ack_latency_samples: 0,
            submit_to_first_report_samples: 0,
            submit_to_fill_samples: 0,
            cancel_ack_latency_samples: 0,
            updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillEvent {
    pub trade_id: String,
    pub order_id: String,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_asset: String,
    pub fee_quote: Option<Decimal>,
    pub liquidity_side: LiquiditySide,
    pub event_time: Timestamp,
}
