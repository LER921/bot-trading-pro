use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, now_utc};
use domain::{
    FeatureSnapshot, HealthState, RegimeDecision, RegimeState, Symbol, SystemHealth,
    VolatilityRegime,
};

#[async_trait]
pub trait RegimeDetector: Send + Sync {
    async fn detect(&self, symbol: Symbol, features: &FeatureSnapshot, health: &SystemHealth) -> Result<RegimeDecision>;
}

#[derive(Debug, Clone)]
pub struct RegimeThresholds {
    pub trend_momentum_bps: Decimal,
    pub imbalance_confirmation: Decimal,
    pub high_volatility_bps: Decimal,
    pub dead_market_spread_bps: Decimal,
    pub dead_market_trade_rate: Decimal,
    pub min_liquidity_score: Decimal,
}

impl Default for RegimeThresholds {
    fn default() -> Self {
        Self {
            trend_momentum_bps: Decimal::from(5u32),
            imbalance_confirmation: Decimal::from_str_exact("0.10").unwrap(),
            high_volatility_bps: Decimal::from(25u32),
            dead_market_spread_bps: Decimal::from(12u32),
            dead_market_trade_rate: Decimal::from_str_exact("0.10").unwrap(),
            min_liquidity_score: Decimal::from(50u32),
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
        let (state, confidence, reason) = if health.market_ws.state != HealthState::Healthy
            || health.user_ws.state != HealthState::Healthy
            || health.clock_drift.state != HealthState::Healthy
            || health.market_data.state != HealthState::Healthy
        {
            (
                RegimeState::RiskOff,
                Decimal::ONE,
                format!(
                    "risk-off: health market_ws={:?} user_ws={:?} clock={:?} market_data={:?}",
                    health.market_ws.state,
                    health.user_ws.state,
                    health.clock_drift.state,
                    health.market_data.state
                ),
            )
        } else if is_dead_market(features, &self.thresholds) {
            (
                RegimeState::DeadMarket,
                Decimal::from_str_exact("0.85").unwrap(),
                format!(
                    "dead-market: spread_bps={} trade_rate={} liquidity_score={}",
                    features.spread_bps,
                    features.trade_rate_per_sec,
                    features.liquidity_score
                ),
            )
        } else if is_high_volatility(features, &self.thresholds) {
            (
                RegimeState::HighVolatility,
                Decimal::from_str_exact("0.90").unwrap(),
                format!(
                    "high-volatility: realized_vol_bps={} regime={:?} toxicity={}",
                    features.realized_volatility_bps,
                    features.volatility_regime,
                    features.toxicity_score
                ),
            )
        } else if is_trend_up(features, &self.thresholds) {
            (
                RegimeState::TrendUp,
                Decimal::from_str_exact("0.80").unwrap(),
                format!(
                    "trend-up: momentum={} trade_flow={} imbalance={}",
                    features.local_momentum_bps,
                    features.trade_flow_imbalance,
                    features.orderbook_imbalance.unwrap_or(Decimal::ZERO)
                ),
            )
        } else if is_trend_down(features, &self.thresholds) {
            (
                RegimeState::TrendDown,
                Decimal::from_str_exact("0.80").unwrap(),
                format!(
                    "trend-down: momentum={} trade_flow={} imbalance={}",
                    features.local_momentum_bps,
                    features.trade_flow_imbalance,
                    features.orderbook_imbalance.unwrap_or(Decimal::ZERO)
                ),
            )
        } else {
            (
                RegimeState::Range,
                Decimal::from_str_exact("0.70").unwrap(),
                format!(
                    "range: spread_zscore={} vol={} vwap_distance={}",
                    features.spread_zscore,
                    features.realized_volatility_bps,
                    features.vwap_distance_bps
                ),
            )
        };

        Ok(RegimeDecision {
            symbol,
            state,
            confidence,
            reason,
            decided_at: now_utc(),
        })
    }
}

fn is_dead_market(features: &FeatureSnapshot, thresholds: &RegimeThresholds) -> bool {
    features.spread_bps >= thresholds.dead_market_spread_bps
        || features.trade_rate_per_sec <= thresholds.dead_market_trade_rate
        || features.liquidity_score <= thresholds.min_liquidity_score
}

fn is_high_volatility(features: &FeatureSnapshot, thresholds: &RegimeThresholds) -> bool {
    features.realized_volatility_bps >= thresholds.high_volatility_bps
        || matches!(features.volatility_regime, VolatilityRegime::High)
        || features.toxicity_score >= Decimal::from_str_exact("0.80").unwrap()
}

fn is_trend_up(features: &FeatureSnapshot, thresholds: &RegimeThresholds) -> bool {
    features.local_momentum_bps >= thresholds.trend_momentum_bps
        && features.trade_flow_imbalance > Decimal::ZERO
        && features.orderbook_imbalance.unwrap_or(Decimal::ZERO) >= thresholds.imbalance_confirmation
}

fn is_trend_down(features: &FeatureSnapshot, thresholds: &RegimeThresholds) -> bool {
    features.local_momentum_bps <= -thresholds.trend_momentum_bps
        && features.trade_flow_imbalance < Decimal::ZERO
        && features.orderbook_imbalance.unwrap_or(Decimal::ZERO) <= -thresholds.imbalance_confirmation
}
