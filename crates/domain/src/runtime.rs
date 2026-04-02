use common::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeState {
    Bootstrap,
    Reconciling,
    Ready,
    Trading,
    Reduced,
    Paused,
    RiskOff,
    Shutdown,
}

impl RuntimeState {
    pub const fn allows_new_orders(self) -> bool {
        matches!(self, Self::Trading | Self::Reduced)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub state: RuntimeState,
    pub entered_at: Timestamp,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeTransition {
    pub from: RuntimeState,
    pub to: RuntimeState,
    pub reason: String,
    pub transitioned_at: Timestamp,
}
