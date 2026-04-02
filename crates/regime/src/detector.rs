use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, now_utc};
use domain::{FeatureSnapshot, HealthState, RegimeDecision, RegimeState, Symbol, SystemHealth};

#[async_trait]
pub trait RegimeDetector: Send + Sync {
    async fn detect(&self, symbol: Symbol, features: &FeatureSnapshot, health: &SystemHealth) -> Result<RegimeDecision>;
}

#[derive(Debug, Clone)]
pub struct RegimeThresholds {
    pub trend_momentum_bps: Decimal,
    pub high_volatility_bps: Decimal,
    pub dead_market_spread_bps: Decimal,
}

impl Default for RegimeThresholds {
    fn default() -> Self {
        Self {
            trend_momentum_bps: Decimal::from(4u32),
            high_volatility_bps: Decimal::from(35u32),
            dead_market_spread_bps: Decimal::from(8u32),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ExecutionRegimeDetector {
    pub thresholds: RegimeThresholds,
}

#[async_trait]
impl RegimeDetector for ExecutionRegimeDetector {
    async fn detect(&self, symbol: Symbol, features: &FeatureSnapshot, health: &SystemHealth) -> Result<RegimeDecision> {
        let state = if !health.market_ws.state.permits_trading()
            || !health.user_ws.state.permits_trading()
            || !health.rest.state.permits_trading()
        {
            RegimeState::RiskOff
        } else if health.market_data.state == HealthState::Stale || health.account_events.state == HealthState::Stale {
            RegimeState::NoTrade
        } else if features.realized_volatility_bps >= self.thresholds.high_volatility_bps {
            RegimeState::HighVolatility
        } else if features.spread_bps >= self.thresholds.dead_market_spread_bps {
            RegimeState::DeadMarket
        } else if features.local_momentum_bps >= self.thresholds.trend_momentum_bps {
            RegimeState::TrendUp
        } else if features.local_momentum_bps <= -self.thresholds.trend_momentum_bps {
            RegimeState::TrendDown
        } else {
            RegimeState::Range
        };

        Ok(RegimeDecision {
            symbol,
            state,
            confidence: Decimal::from(1u32),
            reason: "execution-oriented regime scaffold".to_string(),
            decided_at: now_utc(),
        })
    }
}
