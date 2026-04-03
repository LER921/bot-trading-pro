use crate::Symbol;
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FlowFeatures {
    pub trade_flow_imbalance_250ms: Decimal,
    pub trade_flow_imbalance_500ms: Decimal,
    pub trade_flow_imbalance_1s: Decimal,
    pub market_buy_notional_500ms: Decimal,
    pub market_sell_notional_500ms: Decimal,
    pub market_buy_notional_1s: Decimal,
    pub market_sell_notional_1s: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BookDynamicsFeatures {
    pub orderbook_imbalance_l3: Decimal,
    pub orderbook_imbalance_l5: Decimal,
    pub bid_add_rate: Decimal,
    pub ask_add_rate: Decimal,
    pub bid_cancel_rate: Decimal,
    pub ask_cancel_rate: Decimal,
    pub best_bid_persistence_ms: i64,
    pub best_ask_persistence_ms: i64,
    pub micro_mid_drift_500ms_bps: Decimal,
    pub micro_mid_drift_1s_bps: Decimal,
    pub bid_absorption_score: Decimal,
    pub ask_absorption_score: Decimal,
    pub bid_spoofing_score: Decimal,
    pub ask_spoofing_score: Decimal,
    pub bid_queue_quality: Decimal,
    pub ask_queue_quality: Decimal,
    pub book_instability_score: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MicrostructureSnapshot {
    pub flow: FlowFeatures,
    pub book: BookDynamicsFeatures,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VolatilityRegime {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub symbol: Symbol,
    pub microprice: Option<Decimal>,
    pub top_of_book_depth_quote: Decimal,
    pub orderbook_imbalance: Option<Decimal>,
    pub orderbook_imbalance_rolling: Decimal,
    pub realized_volatility_bps: Decimal,
    pub vwap: Decimal,
    pub vwap_distance_bps: Decimal,
    pub spread_bps: Decimal,
    pub spread_mean_bps: Decimal,
    pub spread_std_bps: Decimal,
    pub spread_zscore: Decimal,
    pub trade_flow_imbalance: Decimal,
    pub trade_flow_rate: Decimal,
    pub trade_rate_per_sec: Decimal,
    pub tick_rate_per_sec: Decimal,
    pub tape_speed: Decimal,
    pub momentum_1s_bps: Decimal,
    pub momentum_5s_bps: Decimal,
    pub momentum_15s_bps: Decimal,
    pub local_momentum_bps: Decimal,
    pub liquidity_score: Decimal,
    pub toxicity_score: Decimal,
    pub microstructure: MicrostructureSnapshot,
    pub volatility_regime: VolatilityRegime,
    pub computed_at: Timestamp,
}
