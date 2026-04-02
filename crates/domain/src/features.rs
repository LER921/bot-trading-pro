use crate::Symbol;
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub symbol: Symbol,
    pub microprice: Option<Decimal>,
    pub orderbook_imbalance: Option<Decimal>,
    pub realized_volatility_bps: Decimal,
    pub vwap_distance_bps: Decimal,
    pub spread_bps: Decimal,
    pub spread_zscore: Decimal,
    pub tape_speed: Decimal,
    pub local_momentum_bps: Decimal,
    pub toxicity_score: Decimal,
    pub computed_at: Timestamp,
}
