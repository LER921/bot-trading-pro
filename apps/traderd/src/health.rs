use common::{now_utc, Timestamp};
use common::clock::measure_drift;
use domain::{
    AccountEventFreshness, ClockDriftHealth, HealthState, MarketDataFreshness, MarketStreamHealth,
    RestHealth, Symbol, SystemHealth, UserStreamHealth, worst_health_state,
};
use exchange_binance_spot::{StreamKind, StreamLifecycle, StreamStatus};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
struct SymbolMarketTimes {
    last_orderbook_update_at: Option<Timestamp>,
    last_trade_update_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamRuntimeState {
    Unknown,
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
}

impl Default for StreamRuntimeState {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, Default)]
struct StreamTracker {
    last_message_at: Option<Timestamp>,
    reconnect_count: u64,
    lifecycle: StreamRuntimeState,
}

#[derive(Debug, Clone, Default)]
pub struct HealthTracker {
    market_ws: StreamTracker,
    user_ws: StreamTracker,
    rest_last_success_at: Option<Timestamp>,
    rest_consecutive_failures: u64,
    rest_last_roundtrip_ms: Option<i64>,
    last_clock_drift_ms: i64,
    last_clock_sample_at: Option<Timestamp>,
    market_times: HashMap<Symbol, SymbolMarketTimes>,
    last_balance_event_at: Option<Timestamp>,
    last_fill_event_at: Option<Timestamp>,
    fallback_active: bool,
}

impl HealthTracker {
    pub fn apply_stream_status(&mut self, status: &StreamStatus) {
        let target = match status.kind {
            StreamKind::MarketWs => &mut self.market_ws,
            StreamKind::UserWs => &mut self.user_ws,
        };

        target.reconnect_count = status.reconnect_count;
        target.lifecycle = match status.lifecycle {
            StreamLifecycle::Connecting => StreamRuntimeState::Connecting,
            StreamLifecycle::Connected => StreamRuntimeState::Connected,
            StreamLifecycle::Reconnecting => StreamRuntimeState::Reconnecting,
            StreamLifecycle::Disconnected => StreamRuntimeState::Disconnected,
        };

        if matches!(status.lifecycle, StreamLifecycle::Connected) {
            target.last_message_at = Some(status.observed_at);
        }
    }

    pub fn set_fallback_active(&mut self, enabled: bool) {
        self.fallback_active = enabled;
    }

    pub fn record_market_ws_message(&mut self, at: Timestamp) {
        self.market_ws.last_message_at = Some(at);
        self.market_ws.lifecycle = StreamRuntimeState::Connected;
    }

    pub fn record_user_ws_message(&mut self, at: Timestamp) {
        self.user_ws.last_message_at = Some(at);
        self.user_ws.lifecycle = StreamRuntimeState::Connected;
    }

    pub fn record_rest_success(&mut self, at: Timestamp, roundtrip_ms: i64) {
        self.rest_last_success_at = Some(at);
        self.rest_consecutive_failures = 0;
        self.rest_last_roundtrip_ms = Some(roundtrip_ms.max(0));
    }

    pub fn record_rest_failure(&mut self) {
        self.rest_consecutive_failures = self.rest_consecutive_failures.saturating_add(1);
    }

    pub fn record_clock_sample(&mut self, exchange_time: Timestamp) -> i64 {
        let local_time = now_utc();
        let drift_ms = measure_drift(local_time, exchange_time);
        self.last_clock_drift_ms = drift_ms;
        self.last_clock_sample_at = Some(local_time);
        drift_ms
    }

    pub fn record_orderbook(&mut self, symbol: Symbol, at: Timestamp) {
        self.market_times
            .entry(symbol)
            .or_default()
            .last_orderbook_update_at = Some(at);
    }

    pub fn record_trade(&mut self, symbol: Symbol, at: Timestamp) {
        self.market_times.entry(symbol).or_default().last_trade_update_at = Some(at);
    }

    pub fn record_account_snapshot(&mut self, at: Timestamp) {
        self.last_balance_event_at = Some(at);
    }

    pub fn record_balance_update(&mut self, at: Timestamp) {
        self.last_balance_event_at = Some(at);
    }

    pub fn record_fill(&mut self, at: Timestamp) {
        self.last_fill_event_at = Some(at);
    }

    pub fn snapshot(
        &self,
        symbols: &[Symbol],
        stale_market_data_ms: u64,
        stale_account_events_ms: u64,
        max_clock_drift_ms: i64,
    ) -> SystemHealth {
        let now = now_utc();
        let market_ws = build_market_stream_health(&self.market_ws, now, stale_market_data_ms);
        let user_ws = build_user_stream_health(&self.user_ws, now, stale_account_events_ms);

        let rest = RestHealth {
            state: if self.rest_consecutive_failures >= 3 {
                HealthState::Unhealthy
            } else if self.rest_consecutive_failures > 0 {
                HealthState::Degraded
            } else {
                HealthState::Healthy
            },
            last_success_at: self.rest_last_success_at,
            consecutive_failures: self.rest_consecutive_failures,
            roundtrip_ms: self.rest_last_roundtrip_ms,
        };

        let clock_drift = ClockDriftHealth {
            state: if self.last_clock_sample_at.is_none() {
                HealthState::Stale
            } else if self.last_clock_drift_ms.abs() <= max_clock_drift_ms {
                HealthState::Healthy
            } else if self.last_clock_drift_ms.abs() <= max_clock_drift_ms * 2 {
                HealthState::Degraded
            } else {
                HealthState::Unhealthy
            },
            drift_ms: self.last_clock_drift_ms,
            sampled_at: self.last_clock_sample_at.unwrap_or(now),
        };

        let mut market_states = Vec::new();
        let mut last_orderbook_update_at = None;
        let mut last_trade_update_at = None;
        for &symbol in symbols {
            let times = self.market_times.get(&symbol);
            let orderbook_at = times.and_then(|times| times.last_orderbook_update_at);
            let trade_at = times.and_then(|times| times.last_trade_update_at);
            market_states.push(derive_age_state(orderbook_at, now, stale_market_data_ms));
            last_orderbook_update_at = max_option_timestamp(last_orderbook_update_at, orderbook_at);
            last_trade_update_at = max_option_timestamp(last_trade_update_at, trade_at);
        }

        let market_data = MarketDataFreshness {
            state: if market_states.is_empty() {
                HealthState::Stale
            } else {
                worst_health_state(market_states)
            },
            last_orderbook_update_at,
            last_trade_update_at,
        };

        let account_reference = max_option_timestamp(
            max_option_timestamp(self.last_balance_event_at, self.last_fill_event_at),
            self.user_ws.last_message_at,
        );
        let account_events = AccountEventFreshness {
            state: derive_age_state(account_reference, now, stale_account_events_ms),
            last_balance_event_at: self.last_balance_event_at,
            last_fill_event_at: self.last_fill_event_at,
        };

        SystemHealth {
            overall_state: HealthState::Healthy,
            market_ws,
            user_ws,
            rest,
            clock_drift,
            market_data,
            account_events,
            fallback_active: self.fallback_active,
            updated_at: now,
        }
        .recompute()
    }
}

fn build_market_stream_health(
    tracker: &StreamTracker,
    now: Timestamp,
    threshold_ms: u64,
) -> MarketStreamHealth {
    let state = derive_stream_state(tracker, now, threshold_ms);
    let last_event_age_ms = tracker
        .last_message_at
        .map(|timestamp| (now - timestamp).whole_milliseconds() as i64);

    MarketStreamHealth {
        state,
        last_message_at: tracker.last_message_at,
        last_event_age_ms,
        reconnect_count: tracker.reconnect_count,
    }
}

fn build_user_stream_health(
    tracker: &StreamTracker,
    now: Timestamp,
    threshold_ms: u64,
) -> UserStreamHealth {
    let state = derive_stream_state(tracker, now, threshold_ms);
    let last_event_age_ms = tracker
        .last_message_at
        .map(|timestamp| (now - timestamp).whole_milliseconds() as i64);

    UserStreamHealth {
        state,
        last_message_at: tracker.last_message_at,
        last_event_age_ms,
        reconnect_count: tracker.reconnect_count,
    }
}

fn derive_stream_state(tracker: &StreamTracker, now: Timestamp, threshold_ms: u64) -> HealthState {
    let age_state = derive_age_state(tracker.last_message_at, now, threshold_ms);
    let lifecycle_state = match tracker.lifecycle {
        StreamRuntimeState::Unknown => HealthState::Stale,
        StreamRuntimeState::Connecting => HealthState::Degraded,
        StreamRuntimeState::Connected => HealthState::Healthy,
        StreamRuntimeState::Reconnecting => HealthState::Degraded,
        StreamRuntimeState::Disconnected => HealthState::Unhealthy,
    };
    worst_health_state([age_state, lifecycle_state])
}

fn derive_age_state(last: Option<Timestamp>, now: Timestamp, threshold_ms: u64) -> HealthState {
    let Some(last) = last else {
        return HealthState::Stale;
    };

    let age = (now - last).whole_milliseconds();
    if age <= threshold_ms as i128 {
        HealthState::Healthy
    } else if age <= (threshold_ms as i128 * 2) {
        HealthState::Degraded
    } else {
        HealthState::Stale
    }
}

fn max_option_timestamp(left: Option<Timestamp>, right: Option<Timestamp>) -> Option<Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left >= right { left } else { right }),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;

    #[test]
    fn marks_market_ws_stale_when_last_message_is_too_old() {
        let mut tracker = HealthTracker::default();
        let stale_at = now_utc() - Duration::seconds(30);
        tracker.record_market_ws_message(stale_at);
        tracker.record_user_ws_message(now_utc());
        tracker.record_rest_success(now_utc(), 42);
        tracker.record_clock_sample(now_utc());
        tracker.record_orderbook(Symbol::BtcUsdc, stale_at);
        tracker.record_account_snapshot(now_utc());

        let health = tracker.snapshot(&[Symbol::BtcUsdc], 1_000, 1_000, 500);
        assert_eq!(health.market_ws.state, HealthState::Stale);
        assert!(health.market_ws.last_event_age_ms.unwrap_or_default() > 1_000);
    }

    #[test]
    fn fallback_mode_does_not_hide_stale_ws_health() {
        let mut tracker = HealthTracker::default();
        let now = now_utc();
        tracker.record_market_ws_message(now - Duration::seconds(30));
        tracker.record_user_ws_message(now - Duration::seconds(30));
        tracker.record_orderbook(Symbol::BtcUsdc, now);
        tracker.record_trade(Symbol::BtcUsdc, now);
        tracker.record_account_snapshot(now);
        tracker.record_fill(now);
        tracker.record_rest_success(now, 42);
        tracker.record_clock_sample(now);
        tracker.set_fallback_active(true);

        let health = tracker.snapshot(&[Symbol::BtcUsdc], 1_000, 1_000, 500);
        assert!(health.fallback_active);
        assert_ne!(health.overall_state, HealthState::Healthy);
    }

    #[test]
    fn fresh_user_stream_heartbeat_keeps_account_channel_healthy_when_account_is_idle() {
        let mut tracker = HealthTracker::default();
        let now = now_utc();
        tracker.record_market_ws_message(now);
        tracker.record_user_ws_message(now);
        tracker.record_orderbook(Symbol::BtcUsdc, now);
        tracker.record_trade(Symbol::BtcUsdc, now);
        tracker.record_rest_success(now, 42);
        tracker.record_clock_sample(now);

        let health = tracker.snapshot(&[Symbol::BtcUsdc], 1_000, 1_000, 500);

        assert_eq!(health.user_ws.state, HealthState::Healthy);
        assert_eq!(health.account_events.state, HealthState::Healthy);
    }
}
