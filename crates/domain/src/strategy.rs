use crate::{
    BestBidAsk, FeatureSnapshot, InventorySnapshot, RegimeDecision, RiskMode, RuntimeState,
    Side, Symbol, TimeInForce,
};
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyKind {
    MarketMaking,
    Scalping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentRole {
    AddRisk,
    ReduceRisk,
    PassiveProfitTake,
    DefensiveExit,
    ForcedUnwind,
    EmergencyExit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitStage {
    Passive,
    Tighten,
    Aggressive,
    Emergency,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyContext {
    pub symbol: Symbol,
    pub best_bid_ask: Option<BestBidAsk>,
    pub features: FeatureSnapshot,
    pub fill_quality: FillQualitySnapshot,
    pub regime: RegimeDecision,
    pub inventory: InventorySnapshot,
    pub soft_inventory_base: Decimal,
    pub max_inventory_base: Decimal,
    pub local_min_notional_quote: Decimal,
    pub open_bot_orders_for_symbol: u32,
    pub max_open_orders_for_symbol: u32,
    pub runtime_state: RuntimeState,
    pub risk_mode: RiskMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FillQualitySnapshot {
    pub avg_markout_500ms_bps: Decimal,
    pub avg_markout_1s_bps: Decimal,
    pub avg_markout_3s_bps: Decimal,
    pub avg_markout_5s_bps: Decimal,
    pub positive_markout_rate: Decimal,
    pub adverse_selection_rate: Decimal,
    pub fill_quality_score: Decimal,
    pub samples: u64,
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
    pub role: IntentRole,
    pub exit_stage: Option<ExitStage>,
    pub exit_reason: Option<String>,
    pub expected_edge_bps: Decimal,
    pub expected_fee_bps: Decimal,
    pub expected_slippage_bps: Decimal,
    pub edge_after_cost_bps: Decimal,
    pub expected_realized_edge_bps: Decimal,
    pub adverse_selection_penalty_bps: Decimal,
    pub setup_type: Option<String>,
    pub size_tier: Option<String>,
    pub reason: String,
    pub created_at: Timestamp,
    pub expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyOutcome {
    pub intents: Vec<TradeIntent>,
    pub standby_reason: Option<String>,
    pub entry_block_reason: Option<String>,
    pub best_expected_realized_edge_bps: Option<Decimal>,
    pub adverse_selection_hits: u64,
}
