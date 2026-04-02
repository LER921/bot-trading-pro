use crate::time::{now_utc, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClockSnapshot {
    pub local_time: Timestamp,
    pub exchange_time: Option<Timestamp>,
    pub drift_ms: Option<i64>,
}

pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        now_utc()
    }
}

pub fn measure_drift(local_time: Timestamp, exchange_time: Timestamp) -> i64 {
    (local_time - exchange_time).whole_milliseconds() as i64
}
