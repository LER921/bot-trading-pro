use crate::Symbol;
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RegimeState {
    Range,
    TrendUp,
    TrendDown,
    HighVolatility,
    DeadMarket,
    NoTrade,
    RiskOff,
}

impl RegimeState {
    pub const fn allows_new_risk(self) -> bool {
        matches!(self, Self::Range | Self::TrendUp | Self::TrendDown)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeDecision {
    pub symbol: Symbol,
    pub state: RegimeState,
    pub confidence: Decimal,
    pub reason: String,
    pub decided_at: Timestamp,
}
