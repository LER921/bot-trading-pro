use crate::Symbol;
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

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
    pub volatility_regime: VolatilityRegime,
    pub computed_at: Timestamp,
}
