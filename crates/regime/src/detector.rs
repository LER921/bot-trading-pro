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
    pub trend_flow_imbalance: Decimal,
    pub trend_flow_imbalance_strong: Decimal,
    pub trend_book_imbalance: Decimal,
    pub trend_book_imbalance_strong: Decimal,
    pub trend_vwap_distance_bps: Decimal,
    pub trend_vwap_distance_strong_bps: Decimal,
    pub high_volatility_bps: Decimal,
    pub extreme_volatility_bps: Decimal,
    pub high_toxicity_score: Decimal,
    pub extreme_toxicity_score: Decimal,
    pub wide_spread_zscore: Decimal,
    pub extreme_spread_zscore: Decimal,
    pub dead_trade_rate_per_sec: Decimal,
    pub active_trade_rate_per_sec: Decimal,
    pub dead_liquidity_score: Decimal,
    pub healthy_liquidity_score: Decimal,
    pub dead_momentum_bps: Decimal,
    pub active_momentum_bps: Decimal,
    pub dead_volatility_bps: Decimal,
    pub active_volatility_bps: Decimal,
    pub dead_inert_spread_zscore: Decimal,
    pub dead_stable_spread_zscore: Decimal,
    pub range_min_score: Decimal,
    pub trend_min_score: Decimal,
    pub high_volatility_min_score: Decimal,
    pub dead_market_min_score: Decimal,
    pub trend_dominance_margin: Decimal,
    pub trend_range_margin: Decimal,
    pub high_volatility_trend_margin: Decimal,
    pub high_volatility_range_margin: Decimal,
    pub dead_market_volatility_margin: Decimal,
    pub dead_market_trend_margin: Decimal,
    pub range_flow_balance_max_abs: Decimal,
    pub range_flow_balance_ideal_abs: Decimal,
    pub range_book_balance_max_abs: Decimal,
    pub range_book_balance_ideal_abs: Decimal,
    pub range_momentum_balance_bps: Decimal,
    pub range_volatility_floor_bps: Decimal,
    pub range_volatility_ceiling_bps: Decimal,
    pub range_spread_stability_high_zscore: Decimal,
    pub range_spread_stability_low_zscore: Decimal,
    pub range_low_toxicity_score: Decimal,
    pub range_toxicity_ideal_score: Decimal,
    pub range_min_liquidity_score: Decimal,
    pub range_directional_pressure_high_score: Decimal,
    pub range_directional_pressure_low_score: Decimal,
}

impl Default for RegimeThresholds {
    fn default() -> Self {
        Self {
            trend_momentum_bps: Decimal::from(6u32),
            trend_flow_imbalance: Decimal::from_str_exact("0.15").unwrap(),
            trend_flow_imbalance_strong: Decimal::from_str_exact("0.55").unwrap(),
            trend_book_imbalance: Decimal::from_str_exact("0.12").unwrap(),
            trend_book_imbalance_strong: Decimal::from_str_exact("0.45").unwrap(),
            trend_vwap_distance_bps: Decimal::from(2u32),
            trend_vwap_distance_strong_bps: Decimal::from(12u32),
            high_volatility_bps: Decimal::from(20u32),
            extreme_volatility_bps: Decimal::from(40u32),
            high_toxicity_score: Decimal::from_str_exact("0.70").unwrap(),
            extreme_toxicity_score: Decimal::from_str_exact("0.95").unwrap(),
            wide_spread_zscore: Decimal::from_str_exact("1.50").unwrap(),
            extreme_spread_zscore: Decimal::from_str_exact("3.00").unwrap(),
            dead_trade_rate_per_sec: Decimal::from_str_exact("0.08").unwrap(),
            active_trade_rate_per_sec: Decimal::from_str_exact("0.35").unwrap(),
            dead_liquidity_score: Decimal::from(500u32),
            healthy_liquidity_score: Decimal::from(3_000u32),
            dead_momentum_bps: Decimal::from_str_exact("0.50").unwrap(),
            active_momentum_bps: Decimal::from(4u32),
            dead_volatility_bps: Decimal::from(3u32),
            active_volatility_bps: Decimal::from(12u32),
            dead_inert_spread_zscore: Decimal::from_str_exact("0.20").unwrap(),
            dead_stable_spread_zscore: Decimal::from_str_exact("1.00").unwrap(),
            range_min_score: Decimal::from_str_exact("0.55").unwrap(),
            trend_min_score: Decimal::from_str_exact("0.64").unwrap(),
            high_volatility_min_score: Decimal::from_str_exact("0.72").unwrap(),
            dead_market_min_score: Decimal::from_str_exact("0.72").unwrap(),
            trend_dominance_margin: Decimal::from_str_exact("0.15").unwrap(),
            trend_range_margin: Decimal::from_str_exact("0.05").unwrap(),
            high_volatility_trend_margin: Decimal::from_str_exact("0.08").unwrap(),
            high_volatility_range_margin: Decimal::from_str_exact("0.05").unwrap(),
            dead_market_volatility_margin: Decimal::from_str_exact("0.08").unwrap(),
            dead_market_trend_margin: Decimal::from_str_exact("0.12").unwrap(),
            range_flow_balance_max_abs: Decimal::from_str_exact("0.30").unwrap(),
            range_flow_balance_ideal_abs: Decimal::from_str_exact("0.05").unwrap(),
            range_book_balance_max_abs: Decimal::from_str_exact("0.30").unwrap(),
            range_book_balance_ideal_abs: Decimal::from_str_exact("0.05").unwrap(),
            range_momentum_balance_bps: Decimal::from(3u32),
            range_volatility_floor_bps: Decimal::from(4u32),
            range_volatility_ceiling_bps: Decimal::from(14u32),
            range_spread_stability_high_zscore: Decimal::from_str_exact("1.20").unwrap(),
            range_spread_stability_low_zscore: Decimal::from_str_exact("0.20").unwrap(),
            range_low_toxicity_score: Decimal::from_str_exact("0.65").unwrap(),
            range_toxicity_ideal_score: Decimal::from_str_exact("0.10").unwrap(),
            range_min_liquidity_score: Decimal::from(1_500u32),
            range_directional_pressure_high_score: Decimal::from_str_exact("0.60").unwrap(),
            range_directional_pressure_low_score: Decimal::from_str_exact("0.15").unwrap(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RegimeScores {
    trend_up: Decimal,
    trend_down: Decimal,
    high_volatility: Decimal,
    dead_market: Decimal,
    range: Decimal,
}

#[derive(Debug, Default, Clone)]
pub struct ExecutionRegimeDetector {
    pub thresholds: RegimeThresholds,
}

#[async_trait]
impl RegimeDetector for ExecutionRegimeDetector {
    async fn detect(&self, symbol: Symbol, features: &FeatureSnapshot, health: &SystemHealth) -> Result<RegimeDecision> {
        if is_risk_off(health) {
            return Ok(RegimeDecision {
                symbol,
                state: RegimeState::RiskOff,
                confidence: Decimal::ONE,
                reason: format!(
                    "risk-off: market_ws={:?} user_ws={:?} market_data={:?} clock={:?} account_events={:?}",
                    health.market_ws.state,
                    health.user_ws.state,
                    health.market_data.state,
                    health.clock_drift.state,
                    health.account_events.state
                ),
                decided_at: now_utc(),
            });
        }

        let scores = score_regimes(features, &self.thresholds);
        let dominant_trend = max_decimal(scores.trend_up, scores.trend_down);

        let (state, leading_score) = if scores.dead_market >= self.thresholds.dead_market_min_score
            && scores.dead_market
                >= scores.high_volatility + self.thresholds.dead_market_volatility_margin
            && scores.dead_market >= dominant_trend + self.thresholds.dead_market_trend_margin
        {
            (RegimeState::DeadMarket, scores.dead_market)
        } else if scores.high_volatility >= self.thresholds.high_volatility_min_score
            && scores.high_volatility
                >= dominant_trend + self.thresholds.high_volatility_trend_margin
            && scores.high_volatility >= scores.range + self.thresholds.high_volatility_range_margin
        {
            (RegimeState::HighVolatility, scores.high_volatility)
        } else if scores.trend_up >= self.thresholds.trend_min_score
            && scores.trend_up >= scores.trend_down + self.thresholds.trend_dominance_margin
            && scores.trend_up >= scores.range + self.thresholds.trend_range_margin
        {
            (RegimeState::TrendUp, scores.trend_up)
        } else if scores.trend_down >= self.thresholds.trend_min_score
            && scores.trend_down >= scores.trend_up + self.thresholds.trend_dominance_margin
            && scores.trend_down >= scores.range + self.thresholds.trend_range_margin
        {
            (RegimeState::TrendDown, scores.trend_down)
        } else {
            (RegimeState::Range, max_decimal(scores.range, self.thresholds.range_min_score))
        };

        Ok(RegimeDecision {
            symbol,
            state,
            confidence: regime_confidence(leading_score),
            reason: format_reason(state, scores, features),
            decided_at: now_utc(),
        })
    }
}

fn is_risk_off(health: &SystemHealth) -> bool {
    health.market_ws.state != HealthState::Healthy
        || health.user_ws.state != HealthState::Healthy
        || health.market_data.state != HealthState::Healthy
        || health.clock_drift.state != HealthState::Healthy
        || health.account_events.state != HealthState::Healthy
}

fn score_regimes(features: &FeatureSnapshot, thresholds: &RegimeThresholds) -> RegimeScores {
    let trend_up = directional_trend_score(features, thresholds, Decimal::ONE);
    let trend_down = directional_trend_score(features, thresholds, -Decimal::ONE);
    let high_volatility = high_volatility_score(features, thresholds);
    let dead_market = dead_market_score(features, thresholds);
    let range = range_score(features, thresholds, trend_up, trend_down, high_volatility, dead_market);

    RegimeScores {
        trend_up,
        trend_down,
        high_volatility,
        dead_market,
        range,
    }
}

fn directional_trend_score(
    features: &FeatureSnapshot,
    thresholds: &RegimeThresholds,
    direction: Decimal,
) -> Decimal {
    let momentum = score_above(
        features.local_momentum_bps * direction,
        thresholds.trend_momentum_bps,
        thresholds.trend_momentum_bps * Decimal::from(3u32),
    );
    let trade_flow = score_above(
        features.trade_flow_imbalance * direction,
        thresholds.trend_flow_imbalance,
        thresholds.trend_flow_imbalance_strong,
    );
    let book = score_above(
        features.orderbook_imbalance.unwrap_or(Decimal::ZERO) * direction,
        thresholds.trend_book_imbalance,
        thresholds.trend_book_imbalance_strong,
    );
    let vwap_distance = score_above(
        features.vwap_distance_bps * direction,
        thresholds.trend_vwap_distance_bps,
        thresholds.trend_vwap_distance_strong_bps,
    );
    let toxicity_penalty = score_above(
        features.toxicity_score,
        thresholds.high_toxicity_score,
        thresholds.extreme_toxicity_score,
    );

    clamp_unit(
        momentum * dec("0.40")
            + trade_flow * dec("0.25")
            + book * dec("0.20")
            + vwap_distance * dec("0.15")
            - toxicity_penalty * dec("0.10"),
    )
}

fn high_volatility_score(features: &FeatureSnapshot, thresholds: &RegimeThresholds) -> Decimal {
    let realized_vol = score_above(
        features.realized_volatility_bps,
        thresholds.high_volatility_bps,
        thresholds.extreme_volatility_bps,
    );
    let volatility_regime = match features.volatility_regime {
        VolatilityRegime::High => Decimal::ONE,
        VolatilityRegime::Medium => dec("0.40"),
        VolatilityRegime::Low => Decimal::ZERO,
    };
    let toxicity = score_above(
        features.toxicity_score,
        thresholds.high_toxicity_score,
        thresholds.extreme_toxicity_score,
    );
    let spread = score_above(
        features.spread_zscore.abs(),
        thresholds.wide_spread_zscore,
        thresholds.extreme_spread_zscore,
    );

    clamp_unit(
        realized_vol * dec("0.40")
            + volatility_regime * dec("0.25")
            + toxicity * dec("0.20")
            + spread * dec("0.15"),
    )
}

fn dead_market_score(features: &FeatureSnapshot, thresholds: &RegimeThresholds) -> Decimal {
    let trade_rate = score_below(
        features.trade_rate_per_sec,
        thresholds.active_trade_rate_per_sec,
        thresholds.dead_trade_rate_per_sec,
    );
    let liquidity = score_below(
        features.liquidity_score,
        thresholds.healthy_liquidity_score,
        thresholds.dead_liquidity_score,
    );
    let momentum = score_below(
        features.local_momentum_bps.abs(),
        thresholds.active_momentum_bps,
        thresholds.dead_momentum_bps,
    );
    let volatility = score_below(
        features.realized_volatility_bps,
        thresholds.active_volatility_bps,
        thresholds.dead_volatility_bps,
    );
    let inert_spread = score_below(
        features.spread_zscore.abs(),
        thresholds.dead_stable_spread_zscore,
        thresholds.dead_inert_spread_zscore,
    );

    clamp_unit(
        trade_rate * dec("0.30")
            + liquidity * dec("0.25")
            + momentum * dec("0.20")
            + volatility * dec("0.15")
            + inert_spread * dec("0.10"),
    )
}

fn range_score(
    features: &FeatureSnapshot,
    thresholds: &RegimeThresholds,
    trend_up: Decimal,
    trend_down: Decimal,
    high_volatility: Decimal,
    dead_market: Decimal,
) -> Decimal {
    let balanced_flow = score_below(
        features.trade_flow_imbalance.abs(),
        thresholds.range_flow_balance_max_abs,
        thresholds.range_flow_balance_ideal_abs,
    );
    let balanced_book = score_below(
        features.orderbook_imbalance.unwrap_or(Decimal::ZERO).abs(),
        thresholds.range_book_balance_max_abs,
        thresholds.range_book_balance_ideal_abs,
    );
    let balanced_momentum = score_below(
        features.local_momentum_bps.abs(),
        thresholds.range_momentum_balance_bps,
        thresholds.dead_momentum_bps,
    );
    let moderate_vol = moderate_range_volatility_score(features, thresholds);
    let stable_spread = score_below(
        features.spread_zscore.abs(),
        thresholds.range_spread_stability_high_zscore,
        thresholds.range_spread_stability_low_zscore,
    );
    let low_toxicity = score_below(
        features.toxicity_score,
        thresholds.range_low_toxicity_score,
        thresholds.range_toxicity_ideal_score,
    );
    let adequate_liquidity = score_above(
        features.liquidity_score,
        thresholds.range_min_liquidity_score,
        thresholds.healthy_liquidity_score,
    );
    let low_directional_pressure = score_below(
        max_decimal(max_decimal(trend_up, trend_down), max_decimal(high_volatility, dead_market)),
        thresholds.range_directional_pressure_high_score,
        thresholds.range_directional_pressure_low_score,
    );

    clamp_unit(
        balanced_flow * dec("0.18")
            + balanced_book * dec("0.16")
            + balanced_momentum * dec("0.14")
            + moderate_vol * dec("0.16")
            + stable_spread * dec("0.12")
            + low_toxicity * dec("0.10")
            + adequate_liquidity * dec("0.09")
            + low_directional_pressure * dec("0.05"),
    )
}

fn moderate_range_volatility_score(
    features: &FeatureSnapshot,
    thresholds: &RegimeThresholds,
) -> Decimal {
    let volatility = features.realized_volatility_bps;
    if volatility >= thresholds.range_volatility_floor_bps
        && volatility <= thresholds.range_volatility_ceiling_bps
    {
        Decimal::ONE
    } else if volatility < thresholds.range_volatility_floor_bps {
        score_above(
            volatility,
            thresholds.dead_volatility_bps,
            thresholds.range_volatility_floor_bps,
        )
    } else {
        score_below(
            volatility,
            thresholds.high_volatility_bps,
            thresholds.range_volatility_ceiling_bps,
        )
    }
}

fn regime_confidence(score: Decimal) -> Decimal {
    clamp_unit(dec("0.55") + score * dec("0.40"))
}

fn format_reason(state: RegimeState, scores: RegimeScores, features: &FeatureSnapshot) -> String {
    format!(
        "{state:?}: scores up={} down={} range={} vol={} dead={} | signals vol_bps={} spread_z={} momentum={} flow={} book={} vwap_dist={} liquidity={} toxicity={} trade_rate={}",
        scores.trend_up,
        scores.trend_down,
        scores.range,
        scores.high_volatility,
        scores.dead_market,
        features.realized_volatility_bps,
        features.spread_zscore,
        features.local_momentum_bps,
        features.trade_flow_imbalance,
        features.orderbook_imbalance.unwrap_or(Decimal::ZERO),
        features.vwap_distance_bps,
        features.liquidity_score,
        features.toxicity_score,
        features.trade_rate_per_sec,
    )
}

fn score_above(value: Decimal, low: Decimal, high: Decimal) -> Decimal {
    if high <= low {
        return Decimal::ZERO;
    }
    if value <= low {
        Decimal::ZERO
    } else if value >= high {
        Decimal::ONE
    } else {
        clamp_unit((value - low) / (high - low))
    }
}

fn score_below(value: Decimal, high: Decimal, low: Decimal) -> Decimal {
    if high <= low {
        return Decimal::ZERO;
    }
    if value >= high {
        Decimal::ZERO
    } else if value <= low {
        Decimal::ONE
    } else {
        clamp_unit((high - value) / (high - low))
    }
}

fn clamp_unit(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        Decimal::ZERO
    } else if value >= Decimal::ONE {
        Decimal::ONE
    } else {
        value
    }
}

fn max_decimal(left: Decimal, right: Decimal) -> Decimal {
    if left >= right { left } else { right }
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{FeatureSnapshot, SystemHealth};
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn features() -> FeatureSnapshot {
        FeatureSnapshot {
            symbol: Symbol::BtcUsdc,
            microprice: Some(dec("60000")),
            top_of_book_depth_quote: dec("50000"),
            orderbook_imbalance: Some(Decimal::ZERO),
            orderbook_imbalance_rolling: Decimal::ZERO,
            realized_volatility_bps: dec("8"),
            vwap: dec("60000"),
            vwap_distance_bps: Decimal::ZERO,
            spread_bps: dec("2"),
            spread_mean_bps: dec("2"),
            spread_std_bps: dec("0.5"),
            spread_zscore: Decimal::ZERO,
            trade_flow_imbalance: Decimal::ZERO,
            trade_flow_rate: dec("0.25"),
            trade_rate_per_sec: dec("0.60"),
            tick_rate_per_sec: dec("0.50"),
            tape_speed: dec("0.60"),
            momentum_1s_bps: Decimal::ZERO,
            momentum_5s_bps: Decimal::ZERO,
            momentum_15s_bps: Decimal::ZERO,
            local_momentum_bps: Decimal::ZERO,
            liquidity_score: dec("6000"),
            toxicity_score: dec("0.15"),
            volatility_regime: VolatilityRegime::Low,
            computed_at: now_utc(),
        }
    }

    fn healthy() -> SystemHealth {
        SystemHealth::healthy(now_utc())
    }

    fn run_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        fn raw_waker() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker { raw_waker() }
            fn wake(_: *const ()) {}
            fn wake_by_ref(_: *const ()) {}
            fn drop(_: *const ()) {}

            RawWaker::new(
                std::ptr::null(),
                &RawWakerVTable::new(clone, wake, wake_by_ref, drop),
            )
        }

        let waker = unsafe { Waker::from_raw(raw_waker()) };
        let mut future = Box::pin(future);
        let mut context = Context::from_waker(&waker);
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("detector future should be immediately ready in tests"),
        }
    }

    #[test]
    fn risk_off_when_account_events_are_degraded() {
        let detector = ExecutionRegimeDetector::default();
        let mut health = healthy();
        health.account_events.state = HealthState::Degraded;
        health = health.recompute();

        let decision = run_ready(detector.detect(Symbol::BtcUsdc, &features(), &health)).unwrap();

        assert_eq!(decision.state, RegimeState::RiskOff);
        assert!(decision.reason.contains("account_events"));
    }

    #[test]
    fn detects_dead_market_from_combined_inertia_signals() {
        let detector = ExecutionRegimeDetector::default();
        let mut snapshot = features();
        snapshot.realized_volatility_bps = dec("1.2");
        snapshot.spread_zscore = dec("0.05");
        snapshot.local_momentum_bps = dec("0.15");
        snapshot.trade_flow_imbalance = dec("0.02");
        snapshot.orderbook_imbalance = Some(dec("0.01"));
        snapshot.liquidity_score = dec("250");
        snapshot.toxicity_score = dec("0.04");
        snapshot.trade_rate_per_sec = dec("0.03");

        let decision = run_ready(detector.detect(Symbol::BtcUsdc, &snapshot, &healthy())).unwrap();

        assert_eq!(decision.state, RegimeState::DeadMarket);
        assert!(decision.reason.contains("dead="));
    }

    #[test]
    fn detects_high_volatility_from_combined_pressure_signals() {
        let detector = ExecutionRegimeDetector::default();
        let mut snapshot = features();
        snapshot.realized_volatility_bps = dec("36");
        snapshot.volatility_regime = VolatilityRegime::High;
        snapshot.toxicity_score = dec("0.82");
        snapshot.spread_zscore = dec("2.20");
        snapshot.local_momentum_bps = dec("3");
        snapshot.trade_rate_per_sec = dec("1.40");

        let decision = run_ready(detector.detect(Symbol::BtcUsdc, &snapshot, &healthy())).unwrap();

        assert_eq!(decision.state, RegimeState::HighVolatility);
        assert!(decision.reason.contains("vol="));
    }

    #[test]
    fn detects_trend_up_from_aligned_signals() {
        let detector = ExecutionRegimeDetector::default();
        let mut snapshot = features();
        snapshot.realized_volatility_bps = dec("10");
        snapshot.volatility_regime = VolatilityRegime::Medium;
        snapshot.local_momentum_bps = dec("14");
        snapshot.trade_flow_imbalance = dec("0.42");
        snapshot.orderbook_imbalance = Some(dec("0.30"));
        snapshot.vwap_distance_bps = dec("9");
        snapshot.spread_zscore = dec("0.40");
        snapshot.toxicity_score = dec("0.18");
        snapshot.liquidity_score = dec("7000");
        snapshot.trade_rate_per_sec = dec("1.10");

        let decision = run_ready(detector.detect(Symbol::BtcUsdc, &snapshot, &healthy())).unwrap();

        assert_eq!(decision.state, RegimeState::TrendUp);
        assert!(decision.reason.contains("up="));
    }

    #[test]
    fn detects_trend_down_from_aligned_signals() {
        let detector = ExecutionRegimeDetector::default();
        let mut snapshot = features();
        snapshot.realized_volatility_bps = dec("11");
        snapshot.volatility_regime = VolatilityRegime::Medium;
        snapshot.local_momentum_bps = dec("-18");
        snapshot.trade_flow_imbalance = dec("-0.45");
        snapshot.orderbook_imbalance = Some(dec("-0.34"));
        snapshot.vwap_distance_bps = dec("-10");
        snapshot.spread_zscore = dec("0.35");
        snapshot.toxicity_score = dec("0.20");
        snapshot.liquidity_score = dec("6500");
        snapshot.trade_rate_per_sec = dec("0.95");

        let decision = run_ready(detector.detect(Symbol::BtcUsdc, &snapshot, &healthy())).unwrap();

        assert_eq!(decision.state, RegimeState::TrendDown);
        assert!(decision.reason.contains("down="));
    }

    #[test]
    fn defaults_to_range_when_other_scores_do_not_dominate() {
        let detector = ExecutionRegimeDetector::default();
        let mut snapshot = features();
        snapshot.realized_volatility_bps = dec("7");
        snapshot.volatility_regime = VolatilityRegime::Low;
        snapshot.local_momentum_bps = dec("1.0");
        snapshot.trade_flow_imbalance = dec("0.04");
        snapshot.orderbook_imbalance = Some(dec("0.03"));
        snapshot.vwap_distance_bps = dec("0.8");
        snapshot.spread_zscore = dec("0.15");
        snapshot.toxicity_score = dec("0.12");
        snapshot.liquidity_score = dec("5500");
        snapshot.trade_rate_per_sec = dec("0.70");

        let decision = run_ready(detector.detect(Symbol::BtcUsdc, &snapshot, &healthy())).unwrap();

        assert_eq!(decision.state, RegimeState::Range);
        assert!(decision.reason.contains("range="));
    }
}
