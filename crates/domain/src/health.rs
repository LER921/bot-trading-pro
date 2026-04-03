use common::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
    Stale,
}

impl HealthState {
    pub const fn permits_trading(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketStreamHealth {
    pub state: HealthState,
    pub last_message_at: Option<Timestamp>,
    pub last_event_age_ms: Option<i64>,
    pub reconnect_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserStreamHealth {
    pub state: HealthState,
    pub last_message_at: Option<Timestamp>,
    pub last_event_age_ms: Option<i64>,
    pub reconnect_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestHealth {
    pub state: HealthState,
    pub last_success_at: Option<Timestamp>,
    pub consecutive_failures: u64,
    pub roundtrip_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClockDriftHealth {
    pub state: HealthState,
    pub drift_ms: i64,
    pub sampled_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataFreshness {
    pub state: HealthState,
    pub last_orderbook_update_at: Option<Timestamp>,
    pub last_trade_update_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountEventFreshness {
    pub state: HealthState,
    pub last_balance_event_at: Option<Timestamp>,
    pub last_fill_event_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemHealth {
    pub overall_state: HealthState,
    pub market_ws: MarketStreamHealth,
    pub user_ws: UserStreamHealth,
    pub rest: RestHealth,
    pub clock_drift: ClockDriftHealth,
    pub market_data: MarketDataFreshness,
    pub account_events: AccountEventFreshness,
    pub fallback_active: bool,
    pub updated_at: Timestamp,
}

impl SystemHealth {
    pub fn healthy(updated_at: Timestamp) -> Self {
        let mut health = Self {
            overall_state: HealthState::Healthy,
            market_ws: MarketStreamHealth {
                state: HealthState::Healthy,
                last_message_at: Some(updated_at),
                last_event_age_ms: Some(0),
                reconnect_count: 0,
            },
            user_ws: UserStreamHealth {
                state: HealthState::Healthy,
                last_message_at: Some(updated_at),
                last_event_age_ms: Some(0),
                reconnect_count: 0,
            },
            rest: RestHealth {
                state: HealthState::Healthy,
                last_success_at: Some(updated_at),
                consecutive_failures: 0,
                roundtrip_ms: Some(0),
            },
            clock_drift: ClockDriftHealth {
                state: HealthState::Healthy,
                drift_ms: 0,
                sampled_at: updated_at,
            },
            market_data: MarketDataFreshness {
                state: HealthState::Healthy,
                last_orderbook_update_at: Some(updated_at),
                last_trade_update_at: Some(updated_at),
            },
            account_events: AccountEventFreshness {
                state: HealthState::Healthy,
                last_balance_event_at: Some(updated_at),
                last_fill_event_at: Some(updated_at),
            },
            fallback_active: false,
            updated_at,
        };
        health.overall_state = health.derive_overall_state();
        health
    }

    pub fn derive_overall_state(&self) -> HealthState {
        if matches!(self.market_ws.state, HealthState::Unhealthy)
            || matches!(self.user_ws.state, HealthState::Unhealthy)
            || matches!(self.rest.state, HealthState::Unhealthy)
            || matches!(self.clock_drift.state, HealthState::Unhealthy)
        {
            return HealthState::Unhealthy;
        }

        worst_health_state([
            self.market_ws.state,
            self.user_ws.state,
            self.rest.state,
            self.clock_drift.state,
            self.market_data.state,
            self.account_events.state,
        ])
    }

    pub fn recompute(mut self) -> Self {
        self.overall_state = self.derive_overall_state();
        self
    }
}

pub fn worst_health_state<I>(states: I) -> HealthState
where
    I: IntoIterator<Item = HealthState>,
{
    states.into_iter().fold(HealthState::Healthy, |worst, state| {
        match (worst, state) {
            (HealthState::Unhealthy, _) | (_, HealthState::Unhealthy) => HealthState::Unhealthy,
            (HealthState::Stale, _) | (_, HealthState::Stale) => HealthState::Stale,
            (HealthState::Degraded, _) | (_, HealthState::Degraded) => HealthState::Degraded,
            _ => HealthState::Healthy,
        }
    })
}
