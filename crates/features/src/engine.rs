use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, now_utc};
use domain::{FeatureSnapshot, MarketSnapshot, Symbol, VolatilityRegime};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

const FEATURE_HISTORY_SECS: i64 = 120;
const TRADE_WINDOW_SECS: i64 = 30;

#[async_trait]
pub trait FeatureEngine: Send + Sync {
    async fn compute(&self, snapshot: &MarketSnapshot) -> Result<FeatureSnapshot>;
}

#[derive(Debug, Clone, Copy)]
struct TimedDecimal {
    at: common::Timestamp,
    value: Decimal,
}

#[derive(Debug, Default, Clone)]
struct SymbolFeatureState {
    mid_history: VecDeque<TimedDecimal>,
    spread_history: VecDeque<TimedDecimal>,
    imbalance_history: VecDeque<TimedDecimal>,
}

#[derive(Debug, Default, Clone)]
pub struct RollingFeatureEngine {
    state: Arc<RwLock<HashMap<Symbol, SymbolFeatureState>>>,
}

pub type SimpleFeatureEngine = RollingFeatureEngine;

#[async_trait]
impl FeatureEngine for RollingFeatureEngine {
    async fn compute(&self, snapshot: &MarketSnapshot) -> Result<FeatureSnapshot> {
        let computed_at = now_utc();
        let book_time = snapshot
            .status
            .last_orderbook_update_at
            .unwrap_or(computed_at);
        let recent_trades = recent_trades(&snapshot.recent_trades, computed_at);
        let current_mid = current_mid(snapshot);
        let current_spread_bps = current_spread_bps(snapshot, current_mid);
        let current_imbalance = current_imbalance(snapshot);

        let mut state = self.state.write().await;
        let entry = state.entry(snapshot.symbol).or_default();
        push_point(&mut entry.mid_history, book_time, current_mid);
        push_point(&mut entry.spread_history, book_time, current_spread_bps);
        push_point(&mut entry.imbalance_history, book_time, current_imbalance);
        prune_points(&mut entry.mid_history, computed_at);
        prune_points(&mut entry.spread_history, computed_at);
        prune_points(&mut entry.imbalance_history, computed_at);

        let vwap = vwap_from_trades(&recent_trades).unwrap_or(current_mid);
        let realized_volatility_bps = realized_volatility_bps(&entry.mid_history);
        let spread_mean_bps = mean(entry.spread_history.iter().map(|point| point.value));
        let spread_std_bps = stddev(entry.spread_history.iter().map(|point| point.value), spread_mean_bps);
        let spread_zscore = zscore(current_spread_bps, spread_mean_bps, spread_std_bps);
        let imbalance_rolling = mean(entry.imbalance_history.iter().map(|point| point.value));
        let trade_flow_imbalance = trade_flow_imbalance(&recent_trades);
        let trade_flow_rate = trade_flow_rate(&recent_trades);
        let trade_rate_per_sec = rate_per_sec(recent_trades.len(), TRADE_WINDOW_SECS);
        let tick_rate_per_sec = tick_rate_per_sec(&recent_trades);
        let momentum_1s_bps = momentum_bps(&entry.mid_history, current_mid, 1);
        let momentum_5s_bps = momentum_bps(&entry.mid_history, current_mid, 5);
        let momentum_15s_bps = momentum_bps(&entry.mid_history, current_mid, 15);
        let local_momentum_bps =
            (momentum_1s_bps * Decimal::from(5u32)
                + momentum_5s_bps * Decimal::from(3u32)
                + momentum_15s_bps * Decimal::from(2u32))
                / Decimal::from(10u32);
        let top_of_book_depth_quote = top_of_book_depth_quote(snapshot);
        let liquidity_score = if current_spread_bps.is_zero() {
            top_of_book_depth_quote
        } else {
            top_of_book_depth_quote / current_spread_bps.max(Decimal::ONE)
        };
        let vwap_distance_bps = bps_distance(current_mid, vwap);
        let volatility_regime = classify_volatility(realized_volatility_bps);
        let toxicity_score = toxicity_score(
            trade_flow_imbalance,
            imbalance_rolling,
            spread_zscore,
            realized_volatility_bps,
        );

        Ok(FeatureSnapshot {
            symbol: snapshot.symbol,
            microprice: snapshot.best_bid_ask.as_ref().and_then(|best| microprice(best)),
            top_of_book_depth_quote,
            orderbook_imbalance: snapshot.best_bid_ask.as_ref().map(|_| current_imbalance),
            orderbook_imbalance_rolling: imbalance_rolling,
            realized_volatility_bps,
            vwap,
            vwap_distance_bps,
            spread_bps: current_spread_bps,
            spread_mean_bps,
            spread_std_bps,
            spread_zscore,
            trade_flow_imbalance,
            trade_flow_rate,
            trade_rate_per_sec,
            tick_rate_per_sec,
            tape_speed: trade_rate_per_sec,
            momentum_1s_bps,
            momentum_5s_bps,
            momentum_15s_bps,
            local_momentum_bps,
            liquidity_score,
            toxicity_score,
            volatility_regime,
            computed_at,
        })
    }
}

fn push_point(points: &mut VecDeque<TimedDecimal>, at: common::Timestamp, value: Decimal) {
    points.push_back(TimedDecimal { at, value });
}

fn prune_points(points: &mut VecDeque<TimedDecimal>, now: common::Timestamp) {
    let cutoff = now - time::Duration::seconds(FEATURE_HISTORY_SECS);
    while matches!(points.front(), Some(point) if point.at < cutoff) {
        points.pop_front();
    }
}

fn recent_trades(trades: &[domain::MarketTrade], now: common::Timestamp) -> Vec<domain::MarketTrade> {
    let cutoff = now - time::Duration::seconds(TRADE_WINDOW_SECS);
    trades
        .iter()
        .filter(|trade| trade.received_at >= cutoff)
        .cloned()
        .collect()
}

fn current_mid(snapshot: &MarketSnapshot) -> Decimal {
    if let Some(best) = &snapshot.best_bid_ask {
        return (best.bid_price + best.ask_price) / Decimal::from(2u32);
    }

    snapshot
        .last_trade
        .as_ref()
        .map(|trade| trade.price)
        .unwrap_or(Decimal::ZERO)
}

fn current_spread_bps(snapshot: &MarketSnapshot, current_mid: Decimal) -> Decimal {
    let Some(best) = &snapshot.best_bid_ask else {
        return Decimal::ZERO;
    };
    if current_mid.is_zero() {
        return Decimal::ZERO;
    }
    ((best.ask_price - best.bid_price) / current_mid) * Decimal::from(10_000u32)
}

fn current_imbalance(snapshot: &MarketSnapshot) -> Decimal {
    let Some(best) = &snapshot.best_bid_ask else {
        return Decimal::ZERO;
    };
    let total_qty = best.bid_quantity + best.ask_quantity;
    if total_qty.is_zero() {
        Decimal::ZERO
    } else {
        (best.bid_quantity - best.ask_quantity) / total_qty
    }
}

fn microprice(best: &domain::BestBidAsk) -> Option<Decimal> {
    let total_qty = best.bid_quantity + best.ask_quantity;
    if total_qty.is_zero() {
        None
    } else {
        Some(
            ((best.ask_price * best.bid_quantity) + (best.bid_price * best.ask_quantity))
                / total_qty,
        )
    }
}

fn vwap_from_trades(trades: &[domain::MarketTrade]) -> Option<Decimal> {
    let total_quantity = trades
        .iter()
        .fold(Decimal::ZERO, |acc, trade| acc + trade.quantity);
    if total_quantity.is_zero() {
        return None;
    }

    let total_notional = trades.iter().fold(Decimal::ZERO, |acc, trade| {
        acc + trade.price * trade.quantity
    });
    Some(total_notional / total_quantity)
}

fn realized_volatility_bps(history: &VecDeque<TimedDecimal>) -> Decimal {
    if history.len() < 2 {
        return Decimal::ZERO;
    }

    let mut squared_returns = Vec::new();
    let mut previous = history.front().map(|point| point.value).unwrap_or(Decimal::ZERO);
    for point in history.iter().skip(1) {
        if previous.is_zero() || point.value.is_zero() {
            previous = point.value;
            continue;
        }

        let return_bps = ((point.value - previous) / previous) * Decimal::from(10_000u32);
        squared_returns.push(return_bps * return_bps);
        previous = point.value;
    }

    if squared_returns.is_empty() {
        return Decimal::ZERO;
    }

    decimal_sqrt(mean(squared_returns.into_iter())).unwrap_or(Decimal::ZERO)
}

fn mean<I>(values: I) -> Decimal
where
    I: IntoIterator<Item = Decimal>,
{
    let mut total = Decimal::ZERO;
    let mut count = 0u64;
    for value in values {
        total += value;
        count += 1;
    }

    if count == 0 {
        Decimal::ZERO
    } else {
        total / Decimal::from(count)
    }
}

fn stddev<I>(values: I, mean_value: Decimal) -> Decimal
where
    I: IntoIterator<Item = Decimal>,
{
    let mut total = Decimal::ZERO;
    let mut count = 0u64;
    for value in values {
        let diff = value - mean_value;
        total += diff * diff;
        count += 1;
    }

    if count <= 1 {
        Decimal::ZERO
    } else {
        decimal_sqrt(total / Decimal::from(count)).unwrap_or(Decimal::ZERO)
    }
}

fn zscore(value: Decimal, mean_value: Decimal, stddev_value: Decimal) -> Decimal {
    if stddev_value.is_zero() {
        Decimal::ZERO
    } else {
        (value - mean_value) / stddev_value
    }
}

fn trade_flow_imbalance(trades: &[domain::MarketTrade]) -> Decimal {
    let buy_qty = trades
        .iter()
        .filter(|trade| matches!(trade.aggressor_side, domain::Side::Buy))
        .fold(Decimal::ZERO, |acc, trade| acc + trade.quantity);
    let sell_qty = trades
        .iter()
        .filter(|trade| matches!(trade.aggressor_side, domain::Side::Sell))
        .fold(Decimal::ZERO, |acc, trade| acc + trade.quantity);
    let total = buy_qty + sell_qty;
    if total.is_zero() {
        Decimal::ZERO
    } else {
        (buy_qty - sell_qty) / total
    }
}

fn trade_flow_rate(trades: &[domain::MarketTrade]) -> Decimal {
    let total_qty = trades
        .iter()
        .fold(Decimal::ZERO, |acc, trade| acc + trade.quantity);
    total_qty / Decimal::from(TRADE_WINDOW_SECS)
}

fn rate_per_sec(count: usize, window_secs: i64) -> Decimal {
    if window_secs <= 0 {
        Decimal::ZERO
    } else {
        Decimal::from(count as u64) / Decimal::from(window_secs)
    }
}

fn tick_rate_per_sec(trades: &[domain::MarketTrade]) -> Decimal {
    if trades.is_empty() {
        return Decimal::ZERO;
    }

    let mut price_changes = 1u64;
    let mut previous = trades[0].price;
    for trade in trades.iter().skip(1) {
        if trade.price != previous {
            price_changes += 1;
            previous = trade.price;
        }
    }

    Decimal::from(price_changes) / Decimal::from(TRADE_WINDOW_SECS)
}

fn momentum_bps(history: &VecDeque<TimedDecimal>, current_mid: Decimal, horizon_secs: i64) -> Decimal {
    if current_mid.is_zero() || history.is_empty() {
        return Decimal::ZERO;
    }

    let target = now_utc() - time::Duration::seconds(horizon_secs);
    let base = history
        .iter()
        .rev()
        .find(|point| point.at <= target)
        .or_else(|| history.front())
        .map(|point| point.value)
        .unwrap_or(current_mid);

    if base.is_zero() {
        Decimal::ZERO
    } else {
        ((current_mid - base) / base) * Decimal::from(10_000u32)
    }
}

fn top_of_book_depth_quote(snapshot: &MarketSnapshot) -> Decimal {
    let Some(best) = &snapshot.best_bid_ask else {
        return Decimal::ZERO;
    };

    (best.bid_price * best.bid_quantity) + (best.ask_price * best.ask_quantity)
}

fn bps_distance(value: Decimal, reference: Decimal) -> Decimal {
    if reference.is_zero() {
        Decimal::ZERO
    } else {
        ((value - reference) / reference) * Decimal::from(10_000u32)
    }
}

fn classify_volatility(realized_volatility_bps: Decimal) -> VolatilityRegime {
    if realized_volatility_bps >= Decimal::from(25u32) {
        VolatilityRegime::High
    } else if realized_volatility_bps >= Decimal::from(8u32) {
        VolatilityRegime::Medium
    } else {
        VolatilityRegime::Low
    }
}

fn toxicity_score(
    trade_flow_imbalance: Decimal,
    imbalance_rolling: Decimal,
    spread_zscore: Decimal,
    realized_volatility_bps: Decimal,
) -> Decimal {
    let trade_flow_component = trade_flow_imbalance.abs() * Decimal::from_str_exact("0.35").unwrap();
    let imbalance_component = imbalance_rolling.abs() * Decimal::from_str_exact("0.25").unwrap();
    let spread_component = (spread_zscore.abs() / Decimal::from(4u32))
        .min(Decimal::ONE)
        * Decimal::from_str_exact("0.20").unwrap();
    let volatility_component = (realized_volatility_bps / Decimal::from(50u32))
        .min(Decimal::ONE)
        * Decimal::from_str_exact("0.20").unwrap();

    (trade_flow_component + imbalance_component + spread_component + volatility_component)
        .min(Decimal::ONE)
}

fn decimal_sqrt(value: Decimal) -> Option<Decimal> {
    if value.is_sign_negative() {
        return None;
    }
    if value.is_zero() {
        return Some(Decimal::ZERO);
    }

    let mut guess = value / Decimal::from(2u32);
    if guess.is_zero() {
        guess = Decimal::ONE;
    }

    for _ in 0..12 {
        guess = (guess + value / guess) / Decimal::from(2u32);
    }
    Some(guess)
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{BestBidAsk, MarketDataStatus, MarketTrade, OrderBookSnapshot, PriceLevel, Side, Symbol};

    fn sample_snapshot() -> MarketSnapshot {
        let now = now_utc();
        let recent_trades = vec![
            MarketTrade {
                symbol: Symbol::BtcUsdc,
                trade_id: "t1".to_string(),
                price: Decimal::from(60_000u32),
                quantity: Decimal::from_str_exact("0.01").unwrap(),
                aggressor_side: Side::Buy,
                event_time: now - time::Duration::seconds(4),
                received_at: now - time::Duration::seconds(4),
            },
            MarketTrade {
                symbol: Symbol::BtcUsdc,
                trade_id: "t2".to_string(),
                price: Decimal::from(60_020u32),
                quantity: Decimal::from_str_exact("0.02").unwrap(),
                aggressor_side: Side::Buy,
                event_time: now - time::Duration::seconds(2),
                received_at: now - time::Duration::seconds(2),
            },
            MarketTrade {
                symbol: Symbol::BtcUsdc,
                trade_id: "t3".to_string(),
                price: Decimal::from(60_015u32),
                quantity: Decimal::from_str_exact("0.03").unwrap(),
                aggressor_side: Side::Sell,
                event_time: now,
                received_at: now,
            },
        ];

        MarketSnapshot {
            symbol: Symbol::BtcUsdc,
            best_bid_ask: Some(BestBidAsk {
                symbol: Symbol::BtcUsdc,
                bid_price: Decimal::from(60_010u32),
                bid_quantity: Decimal::from_str_exact("0.5").unwrap(),
                ask_price: Decimal::from(60_020u32),
                ask_quantity: Decimal::from_str_exact("0.4").unwrap(),
                observed_at: now,
            }),
            orderbook: Some(OrderBookSnapshot {
                symbol: Symbol::BtcUsdc,
                bids: vec![PriceLevel {
                    price: Decimal::from(60_010u32),
                    quantity: Decimal::from_str_exact("0.5").unwrap(),
                }],
                asks: vec![PriceLevel {
                    price: Decimal::from(60_020u32),
                    quantity: Decimal::from_str_exact("0.4").unwrap(),
                }],
                last_update_id: 1,
                exchange_time: Some(now),
                observed_at: now,
            }),
            last_trade: recent_trades.last().cloned(),
            recent_trades,
            status: MarketDataStatus {
                is_stale: false,
                needs_resync: false,
                last_orderbook_update_at: Some(now),
                last_trade_update_at: Some(now),
                last_event_latency_ms: Some(5),
                book_age_ms: Some(1),
                trade_flow_rate: Decimal::from_str_exact("0.006").unwrap(),
                book_crossed: false,
                ws_reconnect_counter: 0,
            },
        }
    }

    #[tokio::test]
    async fn computes_non_stubbed_microstructure_features() {
        let engine = RollingFeatureEngine::default();
        let snapshot = sample_snapshot();

        let features = engine.compute(&snapshot).await.unwrap();
        assert!(features.vwap > Decimal::ZERO);
        assert!(features.top_of_book_depth_quote > Decimal::ZERO);
        assert!(features.trade_rate_per_sec > Decimal::ZERO);
        assert!(features.tick_rate_per_sec > Decimal::ZERO);
    }

    #[tokio::test]
    async fn rolling_history_drives_spread_stats_and_momentum() {
        let engine = RollingFeatureEngine::default();
        let snapshot = sample_snapshot();
        let _ = engine.compute(&snapshot).await.unwrap();

        let mut moved = snapshot.clone();
        moved.best_bid_ask.as_mut().unwrap().bid_price += Decimal::from(20u32);
        moved.best_bid_ask.as_mut().unwrap().ask_price += Decimal::from(20u32);

        let features = engine.compute(&moved).await.unwrap();
        assert!(features.spread_mean_bps >= Decimal::ZERO);
        assert!(features.momentum_1s_bps >= Decimal::ZERO);
    }
}
