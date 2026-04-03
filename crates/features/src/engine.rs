use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, Timestamp, now_utc};
use domain::{
    BookDynamicsFeatures, FeatureSnapshot, FlowFeatures, MarketSnapshot,
    MicrostructureSnapshot, Symbol, VolatilityRegime,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

const FEATURE_HISTORY_SECS: i64 = 120;
const REALIZED_VOL_WINDOW_SECS: i64 = 30;
const VWAP_WINDOW_SECS: i64 = 30;
const SPREAD_WINDOW_SECS: i64 = 60;
const IMBALANCE_WINDOW_SECS: i64 = 30;
const TRADE_RATE_WINDOW_SECS: i64 = 30;
const MOMENTUM_1S_WINDOW_SECS: i64 = 1;
const MOMENTUM_5S_WINDOW_SECS: i64 = 5;
const MOMENTUM_15S_WINDOW_SECS: i64 = 15;
const MIN_RATE_WINDOW_MS: i64 = 1_000;
const FLOW_250MS_WINDOW_MS: i64 = 250;
const FLOW_500MS_WINDOW_MS: i64 = 500;
const FLOW_1S_WINDOW_MS: i64 = 1_000;
const BOOK_ACTIVITY_WINDOW_MS: i64 = 1_000;

#[async_trait]
pub trait FeatureEngine: Send + Sync {
    async fn compute(&self, snapshot: &MarketSnapshot) -> Result<FeatureSnapshot>;
}

#[derive(Debug, Clone, Copy)]
struct TimedDecimal {
    at: Timestamp,
    value: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct TimedBookObservation {
    at: Timestamp,
    best_bid_price: Decimal,
    best_ask_price: Decimal,
    best_bid_quantity: Decimal,
    best_ask_quantity: Decimal,
    micro_mid: Decimal,
    bid_depth_l3_quote: Decimal,
    ask_depth_l3_quote: Decimal,
    bid_depth_l5_quote: Decimal,
    ask_depth_l5_quote: Decimal,
    orderbook_imbalance_l3: Decimal,
    orderbook_imbalance_l5: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct TimedBookActivity {
    at: Timestamp,
    bid_add_quote: Decimal,
    ask_add_quote: Decimal,
    bid_cancel_quote: Decimal,
    ask_cancel_quote: Decimal,
    best_bid_changed: bool,
    best_ask_changed: bool,
}

#[derive(Debug, Default, Clone)]
struct SymbolFeatureState {
    mid_history: VecDeque<TimedDecimal>,
    spread_history: VecDeque<TimedDecimal>,
    imbalance_history: VecDeque<TimedDecimal>,
    book_history: VecDeque<TimedBookObservation>,
    activity_history: VecDeque<TimedBookActivity>,
    best_bid_persist_since: Option<Timestamp>,
    best_ask_persist_since: Option<Timestamp>,
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
        let anchor_time = feature_anchor_time(snapshot, computed_at);
        let current_mid = current_mid(snapshot);
        let current_spread_bps = current_spread_bps(snapshot, current_mid);
        let current_imbalance = current_imbalance(snapshot);
        let vwap_window = trades_in_window(&snapshot.recent_trades, anchor_time, VWAP_WINDOW_SECS);
        let trade_window = trades_in_window(&snapshot.recent_trades, anchor_time, TRADE_RATE_WINDOW_SECS);
        let flow_250ms_window = trades_in_window_ms(&snapshot.recent_trades, anchor_time, FLOW_250MS_WINDOW_MS);
        let flow_500ms_window = trades_in_window_ms(&snapshot.recent_trades, anchor_time, FLOW_500MS_WINDOW_MS);
        let flow_1s_window = trades_in_window_ms(&snapshot.recent_trades, anchor_time, FLOW_1S_WINDOW_MS);
        let current_book_observation = current_book_observation(snapshot, anchor_time);

        let mut state = self.state.write().await;
        let entry = state.entry(snapshot.symbol).or_default();
        let previous_book_observation = entry.book_history.back().copied();
        push_point(&mut entry.mid_history, anchor_time, current_mid);
        push_point(&mut entry.spread_history, anchor_time, current_spread_bps);
        push_point(&mut entry.imbalance_history, anchor_time, current_imbalance);
        prune_points(&mut entry.mid_history, anchor_time);
        prune_points(&mut entry.spread_history, anchor_time);
        prune_points(&mut entry.imbalance_history, anchor_time);
        if let Some(current_book_observation) = current_book_observation {
            update_best_quote_persistence(
                &mut entry.best_bid_persist_since,
                previous_book_observation.map(|observation| observation.best_bid_price),
                current_book_observation.best_bid_price,
                anchor_time,
            );
            update_best_quote_persistence(
                &mut entry.best_ask_persist_since,
                previous_book_observation.map(|observation| observation.best_ask_price),
                current_book_observation.best_ask_price,
                anchor_time,
            );
            push_book_observation(&mut entry.book_history, current_book_observation);
            if let Some(activity) =
                book_activity(previous_book_observation, current_book_observation, anchor_time)
            {
                push_book_activity(&mut entry.activity_history, activity);
            }
        }
        prune_book_observations(&mut entry.book_history, anchor_time);
        prune_book_activity(&mut entry.activity_history, anchor_time);

        let mid_window = points_in_window(&entry.mid_history, anchor_time, REALIZED_VOL_WINDOW_SECS);
        let spread_window = points_in_window(&entry.spread_history, anchor_time, SPREAD_WINDOW_SECS);
        let imbalance_window = points_in_window(&entry.imbalance_history, anchor_time, IMBALANCE_WINDOW_SECS);
        let book_window = book_observations_in_window(&entry.book_history, anchor_time, FEATURE_HISTORY_SECS);
        let book_activity_window =
            book_activity_in_window(&entry.activity_history, anchor_time, BOOK_ACTIVITY_WINDOW_MS);

        let vwap = vwap_from_trades(&vwap_window)
            .or_else(|| snapshot.last_trade.as_ref().map(|trade| trade.price))
            .unwrap_or(current_mid);
        let realized_volatility_bps = realized_volatility_bps(&mid_window, &trade_window);
        let spread_mean_bps = mean_or_fallback(
            spread_window.iter().map(|point| point.value),
            current_spread_bps,
        );
        let spread_std_bps = stddev(spread_window.iter().map(|point| point.value), spread_mean_bps);
        let spread_zscore = zscore(current_spread_bps, spread_mean_bps, spread_std_bps);
        let imbalance_rolling = mean_or_fallback(
            imbalance_window.iter().map(|point| point.value),
            current_imbalance,
        );
        let trade_flow_imbalance = trade_flow_imbalance(&trade_window);
        let trade_flow_rate = trade_flow_rate(&trade_window, anchor_time);
        let trade_rate_per_sec = trade_rate_per_sec(&trade_window, anchor_time);
        let tick_rate_per_sec = tick_rate_per_sec(&trade_window, anchor_time);
        let momentum_1s_bps = momentum_bps(
            &entry.mid_history,
            current_mid,
            anchor_time,
            MOMENTUM_1S_WINDOW_SECS,
        );
        let momentum_5s_bps = momentum_bps(
            &entry.mid_history,
            current_mid,
            anchor_time,
            MOMENTUM_5S_WINDOW_SECS,
        );
        let momentum_15s_bps = momentum_bps(
            &entry.mid_history,
            current_mid,
            anchor_time,
            MOMENTUM_15S_WINDOW_SECS,
        );
        let local_momentum_bps = weighted_momentum(momentum_1s_bps, momentum_5s_bps, momentum_15s_bps);
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
        let flow_features = build_flow_features(
            &flow_250ms_window,
            &flow_500ms_window,
            &flow_1s_window,
        );
        let book_dynamics = build_book_dynamics(
            current_book_observation,
            &book_window,
            &book_activity_window,
            entry.best_bid_persist_since,
            entry.best_ask_persist_since,
            anchor_time,
            &flow_features,
        );

        Ok(FeatureSnapshot {
            symbol: snapshot.symbol,
            microprice: snapshot.best_bid_ask.as_ref().and_then(microprice),
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
            microstructure: MicrostructureSnapshot {
                flow: flow_features,
                book: book_dynamics,
            },
            volatility_regime,
            computed_at,
        })
    }
}

fn feature_anchor_time(snapshot: &MarketSnapshot, fallback: Timestamp) -> Timestamp {
    [
        snapshot.status.last_orderbook_update_at,
        snapshot.status.last_trade_update_at,
        snapshot.best_bid_ask.as_ref().map(|best| best.observed_at),
        snapshot.last_trade.as_ref().map(|trade| trade.received_at),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(fallback)
}

fn push_point(points: &mut VecDeque<TimedDecimal>, at: Timestamp, value: Decimal) {
    match points.back_mut() {
        Some(last) if last.at == at => {
            last.value = value;
        }
        Some(last) if last.at > at => {
            // Ignore out-of-order observations instead of corrupting time ordering.
        }
        _ => points.push_back(TimedDecimal { at, value }),
    }
}

fn prune_points(points: &mut VecDeque<TimedDecimal>, anchor_time: Timestamp) {
    let cutoff = anchor_time - time::Duration::seconds(FEATURE_HISTORY_SECS);
    while matches!(points.front(), Some(point) if point.at < cutoff) {
        points.pop_front();
    }
}

fn points_in_window(
    history: &VecDeque<TimedDecimal>,
    anchor_time: Timestamp,
    window_secs: i64,
) -> Vec<TimedDecimal> {
    let cutoff = anchor_time - time::Duration::seconds(window_secs);
    history
        .iter()
        .copied()
        .filter(|point| point.at >= cutoff && point.at <= anchor_time)
        .collect()
}

fn trades_in_window(
    trades: &[domain::MarketTrade],
    anchor_time: Timestamp,
    window_secs: i64,
) -> Vec<domain::MarketTrade> {
    let cutoff = anchor_time - time::Duration::seconds(window_secs);
    trades
        .iter()
        .filter(|trade| trade.received_at >= cutoff && trade.received_at <= anchor_time)
        .cloned()
        .collect()
}

fn trades_in_window_ms(
    trades: &[domain::MarketTrade],
    anchor_time: Timestamp,
    window_ms: i64,
) -> Vec<domain::MarketTrade> {
    let cutoff = anchor_time - time::Duration::milliseconds(window_ms);
    trades
        .iter()
        .filter(|trade| trade.received_at >= cutoff && trade.received_at <= anchor_time)
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

fn current_book_observation(
    snapshot: &MarketSnapshot,
    at: Timestamp,
) -> Option<TimedBookObservation> {
    let best = snapshot.best_bid_ask.as_ref()?;
    let micro_mid = microprice(best).unwrap_or((best.bid_price + best.ask_price) / Decimal::from(2u32));
    let (bid_depth_l3_quote, ask_depth_l3_quote, orderbook_imbalance_l3) =
        depth_imbalance_from_snapshot(snapshot, 3);
    let (bid_depth_l5_quote, ask_depth_l5_quote, orderbook_imbalance_l5) =
        depth_imbalance_from_snapshot(snapshot, 5);

    Some(TimedBookObservation {
        at,
        best_bid_price: best.bid_price,
        best_ask_price: best.ask_price,
        best_bid_quantity: best.bid_quantity,
        best_ask_quantity: best.ask_quantity,
        micro_mid,
        bid_depth_l3_quote,
        ask_depth_l3_quote,
        bid_depth_l5_quote,
        ask_depth_l5_quote,
        orderbook_imbalance_l3,
        orderbook_imbalance_l5,
    })
}

fn depth_imbalance_from_snapshot(
    snapshot: &MarketSnapshot,
    depth: usize,
) -> (Decimal, Decimal, Decimal) {
    let Some(orderbook) = &snapshot.orderbook else {
        let Some(best) = &snapshot.best_bid_ask else {
            return (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
        };
        let bid_quote = best.bid_price * best.bid_quantity;
        let ask_quote = best.ask_price * best.ask_quantity;
        return (bid_quote, ask_quote, imbalance_from_quotes(bid_quote, ask_quote));
    };

    let bid_quote = orderbook
        .bids
        .iter()
        .take(depth)
        .fold(Decimal::ZERO, |acc, level| acc + level.price * level.quantity);
    let ask_quote = orderbook
        .asks
        .iter()
        .take(depth)
        .fold(Decimal::ZERO, |acc, level| acc + level.price * level.quantity);
    (bid_quote, ask_quote, imbalance_from_quotes(bid_quote, ask_quote))
}

fn imbalance_from_quotes(bid_quote: Decimal, ask_quote: Decimal) -> Decimal {
    let total = bid_quote + ask_quote;
    if total.is_zero() {
        Decimal::ZERO
    } else {
        (bid_quote - ask_quote) / total
    }
}

fn update_best_quote_persistence(
    persist_since: &mut Option<Timestamp>,
    previous_price: Option<Decimal>,
    current_price: Decimal,
    anchor_time: Timestamp,
) {
    match previous_price {
        Some(previous_price) if previous_price == current_price => {
            *persist_since = persist_since.or(Some(anchor_time));
        }
        _ => *persist_since = Some(anchor_time),
    }
}

fn push_book_observation(history: &mut VecDeque<TimedBookObservation>, observation: TimedBookObservation) {
    match history.back_mut() {
        Some(last) if last.at == observation.at => *last = observation,
        Some(last) if last.at > observation.at => {}
        _ => history.push_back(observation),
    }
}

fn push_book_activity(history: &mut VecDeque<TimedBookActivity>, activity: TimedBookActivity) {
    match history.back_mut() {
        Some(last) if last.at == activity.at => *last = activity,
        Some(last) if last.at > activity.at => {}
        _ => history.push_back(activity),
    }
}

fn prune_book_observations(history: &mut VecDeque<TimedBookObservation>, anchor_time: Timestamp) {
    let cutoff = anchor_time - time::Duration::seconds(FEATURE_HISTORY_SECS);
    while matches!(history.front(), Some(point) if point.at < cutoff) {
        history.pop_front();
    }
}

fn prune_book_activity(history: &mut VecDeque<TimedBookActivity>, anchor_time: Timestamp) {
    let cutoff = anchor_time - time::Duration::seconds(FEATURE_HISTORY_SECS);
    while matches!(history.front(), Some(point) if point.at < cutoff) {
        history.pop_front();
    }
}

fn book_observations_in_window(
    history: &VecDeque<TimedBookObservation>,
    anchor_time: Timestamp,
    window_secs: i64,
) -> Vec<TimedBookObservation> {
    let cutoff = anchor_time - time::Duration::seconds(window_secs);
    history
        .iter()
        .copied()
        .filter(|point| point.at >= cutoff && point.at <= anchor_time)
        .collect()
}

fn book_activity_in_window(
    history: &VecDeque<TimedBookActivity>,
    anchor_time: Timestamp,
    window_ms: i64,
) -> Vec<TimedBookActivity> {
    let cutoff = anchor_time - time::Duration::milliseconds(window_ms);
    history
        .iter()
        .copied()
        .filter(|point| point.at >= cutoff && point.at <= anchor_time)
        .collect()
}

fn book_activity(
    previous: Option<TimedBookObservation>,
    current: TimedBookObservation,
    at: Timestamp,
) -> Option<TimedBookActivity> {
    let previous = previous?;
    let bid_delta = current.bid_depth_l3_quote - previous.bid_depth_l3_quote;
    let ask_delta = current.ask_depth_l3_quote - previous.ask_depth_l3_quote;
    Some(TimedBookActivity {
        at,
        bid_add_quote: bid_delta.max(Decimal::ZERO),
        ask_add_quote: ask_delta.max(Decimal::ZERO),
        bid_cancel_quote: (-bid_delta).max(Decimal::ZERO),
        ask_cancel_quote: (-ask_delta).max(Decimal::ZERO),
        best_bid_changed: previous.best_bid_price != current.best_bid_price,
        best_ask_changed: previous.best_ask_price != current.best_ask_price,
    })
}

fn build_flow_features(
    flow_250ms_window: &[domain::MarketTrade],
    flow_500ms_window: &[domain::MarketTrade],
    flow_1s_window: &[domain::MarketTrade],
) -> FlowFeatures {
    let (buy_500ms, sell_500ms) = split_trade_notional(flow_500ms_window);
    let (buy_1s, sell_1s) = split_trade_notional(flow_1s_window);
    FlowFeatures {
        trade_flow_imbalance_250ms: trade_flow_imbalance(flow_250ms_window),
        trade_flow_imbalance_500ms: trade_flow_imbalance(flow_500ms_window),
        trade_flow_imbalance_1s: trade_flow_imbalance(flow_1s_window),
        market_buy_notional_500ms: buy_500ms,
        market_sell_notional_500ms: sell_500ms,
        market_buy_notional_1s: buy_1s,
        market_sell_notional_1s: sell_1s,
    }
}

fn split_trade_notional(trades: &[domain::MarketTrade]) -> (Decimal, Decimal) {
    let buy = trades
        .iter()
        .filter(|trade| matches!(trade.aggressor_side, domain::Side::Buy))
        .fold(Decimal::ZERO, |acc, trade| acc + trade.price * trade.quantity);
    let sell = trades
        .iter()
        .filter(|trade| matches!(trade.aggressor_side, domain::Side::Sell))
        .fold(Decimal::ZERO, |acc, trade| acc + trade.price * trade.quantity);
    (buy, sell)
}

fn build_book_dynamics(
    current: Option<TimedBookObservation>,
    history: &[TimedBookObservation],
    activity: &[TimedBookActivity],
    best_bid_persist_since: Option<Timestamp>,
    best_ask_persist_since: Option<Timestamp>,
    anchor_time: Timestamp,
    flow: &FlowFeatures,
) -> BookDynamicsFeatures {
    let Some(current) = current else {
        return BookDynamicsFeatures::default();
    };
    let book_rate_window_secs = Decimal::from(BOOK_ACTIVITY_WINDOW_MS.max(MIN_RATE_WINDOW_MS))
        / Decimal::from(1_000u32);
    let bid_add_rate = activity
        .iter()
        .fold(Decimal::ZERO, |acc, point| acc + point.bid_add_quote)
        / book_rate_window_secs;
    let ask_add_rate = activity
        .iter()
        .fold(Decimal::ZERO, |acc, point| acc + point.ask_add_quote)
        / book_rate_window_secs;
    let bid_cancel_rate = activity
        .iter()
        .fold(Decimal::ZERO, |acc, point| acc + point.bid_cancel_quote)
        / book_rate_window_secs;
    let ask_cancel_rate = activity
        .iter()
        .fold(Decimal::ZERO, |acc, point| acc + point.ask_cancel_quote)
        / book_rate_window_secs;
    let best_bid_persistence_ms = best_bid_persist_since
        .map(|since| (anchor_time - since).whole_milliseconds() as i64)
        .unwrap_or_default();
    let best_ask_persistence_ms = best_ask_persist_since
        .map(|since| (anchor_time - since).whole_milliseconds() as i64)
        .unwrap_or_default();
    let micro_mid_drift_500ms_bps =
        drift_bps(history, current.micro_mid, anchor_time, time::Duration::milliseconds(500));
    let micro_mid_drift_1s_bps =
        drift_bps(history, current.micro_mid, anchor_time, time::Duration::seconds(1));
    let best_quote_change_count = activity
        .iter()
        .filter(|point| point.best_bid_changed || point.best_ask_changed)
        .count() as u64;
    let flicker_rate = Decimal::from(best_quote_change_count) / book_rate_window_secs;
    let bid_absorption_score = clamp_unit(
        normalized_score((-flow.trade_flow_imbalance_250ms).max(Decimal::ZERO), dec("0.05"), dec("0.55"))
            * dec("0.40")
            + normalized_score(current.orderbook_imbalance_l3, dec("0.02"), dec("0.25")) * dec("0.25")
            + normalized_score(
                (bid_add_rate - bid_cancel_rate).max(Decimal::ZERO),
                dec("0.0"),
                dec("2500"),
            ) * dec("0.20")
            + normalized_score(Decimal::from(best_bid_persistence_ms), dec("80"), dec("1000")) * dec("0.15"),
    );
    let ask_absorption_score = clamp_unit(
        normalized_score(flow.trade_flow_imbalance_250ms.max(Decimal::ZERO), dec("0.05"), dec("0.55"))
            * dec("0.40")
            + normalized_score((-current.orderbook_imbalance_l3).max(Decimal::ZERO), dec("0.02"), dec("0.25"))
                * dec("0.25")
            + normalized_score(
                (ask_add_rate - ask_cancel_rate).max(Decimal::ZERO),
                dec("0.0"),
                dec("2500"),
            ) * dec("0.20")
            + normalized_score(Decimal::from(best_ask_persistence_ms), dec("80"), dec("1000")) * dec("0.15"),
    );
    let bid_spoofing_score = clamp_unit(
        normalized_score(bid_add_rate, dec("1500"), dec("7000")) * dec("0.45")
            + normalized_score(bid_cancel_rate, dec("1200"), dec("6500")) * dec("0.35")
            + (Decimal::ONE - normalized_score(Decimal::from(best_bid_persistence_ms), dec("40"), dec("400")))
                * dec("0.20"),
    );
    let ask_spoofing_score = clamp_unit(
        normalized_score(ask_add_rate, dec("1500"), dec("7000")) * dec("0.45")
            + normalized_score(ask_cancel_rate, dec("1200"), dec("6500")) * dec("0.35")
            + (Decimal::ONE - normalized_score(Decimal::from(best_ask_persistence_ms), dec("40"), dec("400")))
                * dec("0.20"),
    );
    let bid_queue_quality = clamp_unit(
        normalized_score(current.orderbook_imbalance_l3, dec("0.00"), dec("0.35")) * dec("0.40")
            + normalized_score(Decimal::from(best_bid_persistence_ms), dec("80"), dec("1200")) * dec("0.25")
            + normalized_score(
                (bid_add_rate - bid_cancel_rate).max(Decimal::ZERO),
                dec("0.0"),
                dec("2500"),
            ) * dec("0.20")
            + (Decimal::ONE - bid_spoofing_score) * dec("0.15"),
    );
    let ask_queue_quality = clamp_unit(
        normalized_score((-current.orderbook_imbalance_l3).max(Decimal::ZERO), dec("0.00"), dec("0.35"))
            * dec("0.40")
            + normalized_score(Decimal::from(best_ask_persistence_ms), dec("80"), dec("1200")) * dec("0.25")
            + normalized_score(
                (ask_add_rate - ask_cancel_rate).max(Decimal::ZERO),
                dec("0.0"),
                dec("2500"),
            ) * dec("0.20")
            + (Decimal::ONE - ask_spoofing_score) * dec("0.15"),
    );
    let book_instability_score = clamp_unit(
        normalized_score(bid_cancel_rate + ask_cancel_rate, dec("1500"), dec("9000")) * dec("0.35")
            + normalized_score(flicker_rate, dec("1.0"), dec("8.0")) * dec("0.25")
            + normalized_score((current.orderbook_imbalance_l3 - current.orderbook_imbalance_l5).abs(), dec("0.03"), dec("0.25"))
                * dec("0.20")
            + normalized_score(
                micro_mid_drift_500ms_bps.abs() + micro_mid_drift_1s_bps.abs(),
                dec("0.5"),
                dec("6.0"),
            ) * dec("0.20"),
    );

    BookDynamicsFeatures {
        orderbook_imbalance_l3: current.orderbook_imbalance_l3,
        orderbook_imbalance_l5: current.orderbook_imbalance_l5,
        bid_add_rate,
        ask_add_rate,
        bid_cancel_rate,
        ask_cancel_rate,
        best_bid_persistence_ms,
        best_ask_persistence_ms,
        micro_mid_drift_500ms_bps,
        micro_mid_drift_1s_bps,
        bid_absorption_score,
        ask_absorption_score,
        bid_spoofing_score,
        ask_spoofing_score,
        bid_queue_quality,
        ask_queue_quality,
        book_instability_score,
    }
}

fn drift_bps(
    history: &[TimedBookObservation],
    current_value: Decimal,
    anchor_time: Timestamp,
    horizon: time::Duration,
) -> Decimal {
    if current_value.is_zero() || history.is_empty() {
        return Decimal::ZERO;
    }
    let target = anchor_time - horizon;
    let base = history
        .iter()
        .rev()
        .find(|point| point.at <= target)
        .or_else(|| history.first())
        .map(|point| point.micro_mid)
        .unwrap_or(current_value);
    bps_distance(current_value, base)
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

fn realized_volatility_bps(
    mid_window: &[TimedDecimal],
    trade_window: &[domain::MarketTrade],
) -> Decimal {
    rms_return_bps(mid_window.iter().map(|point| point.value)).unwrap_or_else(|| {
        rms_return_bps(trade_window.iter().map(|trade| trade.price)).unwrap_or(Decimal::ZERO)
    })
}

fn rms_return_bps<I>(series: I) -> Option<Decimal>
where
    I: IntoIterator<Item = Decimal>,
{
    let mut values = series.into_iter().filter(|value| !value.is_zero());
    let mut previous = values.next()?;
    let mut squared_returns = Vec::new();

    for value in values {
        let return_bps = ((value - previous) / previous) * Decimal::from(10_000u32);
        squared_returns.push(return_bps * return_bps);
        previous = value;
    }

    if squared_returns.is_empty() {
        None
    } else {
        decimal_sqrt(mean(squared_returns.into_iter()))
    }
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

fn mean_or_fallback<I>(values: I, fallback: Decimal) -> Decimal
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
        fallback
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
    let buy_notional = trades
        .iter()
        .filter(|trade| matches!(trade.aggressor_side, domain::Side::Buy))
        .fold(Decimal::ZERO, |acc, trade| acc + trade.price * trade.quantity);
    let sell_notional = trades
        .iter()
        .filter(|trade| matches!(trade.aggressor_side, domain::Side::Sell))
        .fold(Decimal::ZERO, |acc, trade| acc + trade.price * trade.quantity);
    let total_notional = buy_notional + sell_notional;

    if total_notional.is_zero() {
        Decimal::ZERO
    } else {
        (buy_notional - sell_notional) / total_notional
    }
}

fn trade_flow_rate(trades: &[domain::MarketTrade], anchor_time: Timestamp) -> Decimal {
    if trades.is_empty() {
        return Decimal::ZERO;
    }

    let total_quantity = trades
        .iter()
        .fold(Decimal::ZERO, |acc, trade| acc + trade.quantity);
    total_quantity / effective_window_secs(trades, anchor_time)
}

fn trade_rate_per_sec(trades: &[domain::MarketTrade], anchor_time: Timestamp) -> Decimal {
    if trades.is_empty() {
        return Decimal::ZERO;
    }

    Decimal::from(trades.len() as u64) / effective_window_secs(trades, anchor_time)
}

fn tick_rate_per_sec(trades: &[domain::MarketTrade], anchor_time: Timestamp) -> Decimal {
    if trades.len() < 2 {
        return Decimal::ZERO;
    }

    let transitions = trades
        .windows(2)
        .filter(|pair| pair[0].price != pair[1].price)
        .count() as u64;
    Decimal::from(transitions) / effective_window_secs(trades, anchor_time)
}

fn effective_window_secs(trades: &[domain::MarketTrade], anchor_time: Timestamp) -> Decimal {
    let earliest = trades
        .iter()
        .map(|trade| trade.received_at)
        .min()
        .unwrap_or(anchor_time);
    let elapsed_ms = ((anchor_time - earliest).whole_milliseconds() as i64).max(MIN_RATE_WINDOW_MS);
    Decimal::from(elapsed_ms) / Decimal::from(1_000u32)
}

fn momentum_bps(
    history: &VecDeque<TimedDecimal>,
    current_mid: Decimal,
    anchor_time: Timestamp,
    horizon_secs: i64,
) -> Decimal {
    if current_mid.is_zero() || history.is_empty() {
        return Decimal::ZERO;
    }

    let target = anchor_time - time::Duration::seconds(horizon_secs);
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

fn weighted_momentum(momentum_1s_bps: Decimal, momentum_5s_bps: Decimal, momentum_15s_bps: Decimal) -> Decimal {
    (momentum_1s_bps * Decimal::from(5u32)
        + momentum_5s_bps * Decimal::from(3u32)
        + momentum_15s_bps * Decimal::from(2u32))
        / Decimal::from(10u32)
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

fn normalized_score(value: Decimal, low: Decimal, high: Decimal) -> Decimal {
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

fn clamp_unit(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        Decimal::ZERO
    } else if value >= Decimal::ONE {
        Decimal::ONE
    } else {
        value
    }
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{BestBidAsk, MarketDataStatus, MarketTrade, OrderBookSnapshot, PriceLevel, Side, Symbol};

    fn dec(raw: &str) -> Decimal {
        Decimal::from_str_exact(raw).unwrap()
    }

    fn trade(id: &str, price: &str, qty: &str, side: Side, at: Timestamp) -> MarketTrade {
        MarketTrade {
            symbol: Symbol::BtcUsdc,
            trade_id: id.to_string(),
            price: dec(price),
            quantity: dec(qty),
            aggressor_side: side,
            event_time: at,
            received_at: at,
        }
    }

    fn snapshot(
        observed_at: Timestamp,
        bid_price: &str,
        ask_price: &str,
        trades: Vec<MarketTrade>,
    ) -> MarketSnapshot {
        let best_bid_ask = BestBidAsk {
            symbol: Symbol::BtcUsdc,
            bid_price: dec(bid_price),
            bid_quantity: dec("1.0"),
            ask_price: dec(ask_price),
            ask_quantity: dec("1.2"),
            observed_at,
        };

        let orderbook = OrderBookSnapshot {
            symbol: Symbol::BtcUsdc,
            bids: vec![PriceLevel {
                price: dec(bid_price),
                quantity: dec("1.0"),
            }],
            asks: vec![PriceLevel {
                price: dec(ask_price),
                quantity: dec("1.2"),
            }],
            last_update_id: 1,
            exchange_time: Some(observed_at),
            observed_at,
        };

        let last_trade = trades.last().cloned();
        let last_trade_update_at = trades.last().map(|trade| trade.received_at);

        MarketSnapshot {
            symbol: Symbol::BtcUsdc,
            best_bid_ask: Some(best_bid_ask),
            orderbook: Some(orderbook),
            last_trade,
            recent_trades: trades,
            status: MarketDataStatus {
                is_stale: false,
                needs_resync: false,
                last_orderbook_update_at: Some(observed_at),
                last_trade_update_at,
                last_event_latency_ms: Some(5),
                book_age_ms: Some(1),
                trade_flow_rate: Decimal::ZERO,
                book_crossed: false,
                ws_reconnect_counter: 0,
            },
        }
    }

    fn assert_close(actual: Decimal, expected: Decimal, tolerance: Decimal) {
        let diff = (actual - expected).abs();
        assert!(
            diff <= tolerance,
            "expected {actual} to be within {tolerance} of {expected}, diff={diff}"
        );
    }

    #[tokio::test]
    async fn realized_volatility_uses_recent_trade_window_before_book_history_is_warm() {
        let engine = RollingFeatureEngine::default();
        let base = now_utc() - time::Duration::seconds(5);
        let trades = vec![
            trade("t1", "100.00", "1.0", Side::Buy, base - time::Duration::seconds(4)),
            trade("t2", "100.30", "1.0", Side::Buy, base - time::Duration::seconds(3)),
            trade("t3", "100.10", "1.0", Side::Sell, base - time::Duration::seconds(2)),
            trade("t4", "100.60", "1.0", Side::Buy, base),
        ];

        let features = engine
            .compute(&snapshot(base, "100.55", "100.65", trades))
            .await
            .unwrap();

        assert!(features.realized_volatility_bps > Decimal::ZERO);
    }

    #[tokio::test]
    async fn computes_vwap_and_vwap_distance_from_trade_window() {
        let engine = RollingFeatureEngine::default();
        let now = now_utc();
        let trades = vec![
            trade("t1", "100.00", "1.0", Side::Buy, now - time::Duration::seconds(4)),
            trade("t2", "101.00", "3.0", Side::Sell, now - time::Duration::seconds(1)),
        ];

        let features = engine
            .compute(&snapshot(now, "100.75", "100.85", trades))
            .await
            .unwrap();

        assert_close(features.vwap, dec("100.75"), dec("0.0001"));
        assert_close(features.vwap_distance_bps, dec("4.9627791563"), dec("0.01"));
    }

    #[tokio::test]
    async fn computes_local_momentum_on_multiple_short_horizons() {
        let engine = RollingFeatureEngine::default();
        let base = now_utc() - time::Duration::seconds(20);

        let snapshots = vec![
            (base, "99.95", "100.05"),
            (base + time::Duration::seconds(10), "100.95", "101.05"),
            (base + time::Duration::seconds(15), "101.95", "102.05"),
            (base + time::Duration::seconds(19), "102.95", "103.05"),
            (base + time::Duration::seconds(20), "103.95", "104.05"),
        ];

        let mut final_features = None;
        for (at, bid, ask) in snapshots {
            final_features = Some(engine.compute(&snapshot(at, bid, ask, Vec::new())).await.unwrap());
        }

        let features = final_features.unwrap();
        assert!(features.momentum_1s_bps > Decimal::ZERO);
        assert!(features.momentum_5s_bps > features.momentum_1s_bps);
        assert!(features.momentum_15s_bps > features.momentum_5s_bps);
        assert!(features.local_momentum_bps > features.momentum_1s_bps);
    }

    #[tokio::test]
    async fn computes_spread_mean_std_and_zscore_from_rolling_window() {
        let engine = RollingFeatureEngine::default();
        let base = now_utc() - time::Duration::seconds(3);

        let _ = engine
            .compute(&snapshot(base, "99.95", "100.05", Vec::new()))
            .await
            .unwrap();
        let _ = engine
            .compute(&snapshot(base + time::Duration::seconds(1), "99.90", "100.10", Vec::new()))
            .await
            .unwrap();
        let features = engine
            .compute(&snapshot(
                base + time::Duration::seconds(2),
                "99.85",
                "100.15",
                Vec::new(),
            ))
            .await
            .unwrap();

        assert_close(features.spread_mean_bps, dec("20"), dec("0.05"));
        assert_close(features.spread_std_bps, dec("8.1649658093"), dec("0.05"));
        assert_close(features.spread_zscore, dec("1.2247448714"), dec("0.05"));
    }

    #[tokio::test]
    async fn computes_trade_flow_imbalance_from_signed_quote_flow() {
        let engine = RollingFeatureEngine::default();
        let now = now_utc();
        let trades = vec![
            trade("t1", "100.00", "2.0", Side::Buy, now - time::Duration::seconds(3)),
            trade("t2", "100.00", "1.0", Side::Buy, now - time::Duration::seconds(2)),
            trade("t3", "100.00", "1.0", Side::Sell, now - time::Duration::seconds(1)),
        ];

        let features = engine
            .compute(&snapshot(now, "99.95", "100.05", trades))
            .await
            .unwrap();

        assert_close(features.trade_flow_imbalance, dec("0.5"), dec("0.0001"));
    }

    #[tokio::test]
    async fn computes_trade_rate_and_tick_rate_from_effective_window() {
        let engine = RollingFeatureEngine::default();
        let now = now_utc();
        let trades = vec![
            trade("t1", "100.00", "1.0", Side::Buy, now - time::Duration::seconds(4)),
            trade("t2", "100.00", "1.0", Side::Buy, now - time::Duration::seconds(3)),
            trade("t3", "101.00", "1.0", Side::Sell, now - time::Duration::seconds(1)),
            trade("t4", "102.00", "1.0", Side::Buy, now),
        ];

        let features = engine
            .compute(&snapshot(now, "101.95", "102.05", trades))
            .await
            .unwrap();

        assert_close(features.trade_rate_per_sec, dec("1.0"), dec("0.0001"));
        assert_close(features.tick_rate_per_sec, dec("0.5"), dec("0.0001"));
    }
}
