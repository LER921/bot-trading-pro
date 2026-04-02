use crate::{Symbol, TradeIntent};
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskMode {
    #[default]
    Normal,
    Reduced,
    ReduceOnly,
    Paused,
    RiskOff,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventoryControl {
    pub quote_skew_bps_per_inventory_unit: Decimal,
    pub neutralization_clip_fraction: Decimal,
    pub reduce_only_trigger_ratio: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolRiskLimits {
    pub symbol: Symbol,
    pub max_inventory_base: Decimal,
    pub soft_inventory_base: Decimal,
    pub max_quote_notional: Decimal,
    pub max_open_orders: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskLimits {
    pub per_symbol: Vec<SymbolRiskLimits>,
    pub inventory: InventoryControl,
    pub max_daily_loss_usdc: Decimal,
    pub max_symbol_drawdown_usdc: Decimal,
    pub max_total_exposure_quote: Decimal,
    pub max_reject_rate: Decimal,
    pub stale_market_data_ms: u64,
    pub stale_account_events_ms: u64,
    pub max_clock_drift_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskAction {
    Approve,
    Resize,
    Block,
    ReduceOnly,
    CancelAll,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskDecision {
    pub action: RiskAction,
    pub original_intent: TradeIntent,
    pub approved_intent: Option<TradeIntent>,
    pub reason: String,
    pub resulting_mode: RiskMode,
    pub decided_at: Timestamp,
}
