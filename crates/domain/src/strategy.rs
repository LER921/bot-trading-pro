use crate::{
    BestBidAsk, FeatureSnapshot, InventorySnapshot, RegimeDecision, RiskMode, RuntimeState, Side,
    Symbol, TimeInForce,
};
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyKind {
    MarketMaking,
    Scalping,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyContext {
    pub symbol: Symbol,
    pub best_bid_ask: Option<BestBidAsk>,
    pub features: FeatureSnapshot,
    pub regime: RegimeDecision,
    pub inventory: InventorySnapshot,
    pub soft_inventory_base: Decimal,
    pub max_inventory_base: Decimal,
    pub runtime_state: RuntimeState,
    pub risk_mode: RiskMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeIntent {
    pub intent_id: String,
    pub symbol: Symbol,
    pub strategy: StrategyKind,
    pub side: Side,
    pub quantity: Decimal,
    pub limit_price: Option<Decimal>,
    pub max_slippage_bps: Decimal,
    pub post_only: bool,
    pub reduce_only: bool,
    pub time_in_force: Option<TimeInForce>,
    pub expected_edge_bps: Decimal,
    pub expected_fee_bps: Decimal,
    pub expected_slippage_bps: Decimal,
    pub edge_after_cost_bps: Decimal,
    pub reason: String,
    pub created_at: Timestamp,
    pub expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyOutcome {
    pub intents: Vec<TradeIntent>,
    pub standby_reason: Option<String>,
}
