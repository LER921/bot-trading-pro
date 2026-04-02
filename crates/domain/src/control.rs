use crate::{RuntimeState, Symbol};
use common::Timestamp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperatorCommand {
    Pause,
    Resume,
    EnterReduced,
    EnterRiskOff,
    CancelAll { symbol: Option<Symbol> },
    Flatten { symbol: Symbol },
    ReloadConfig,
    Shutdown,
}

impl OperatorCommand {
    pub const fn target_state(&self, current: RuntimeState) -> RuntimeState {
        match self {
            Self::Pause => RuntimeState::Paused,
            Self::Resume => RuntimeState::Trading,
            Self::EnterReduced => RuntimeState::Reduced,
            Self::EnterRiskOff => RuntimeState::RiskOff,
            Self::CancelAll { .. } | Self::Flatten { .. } | Self::ReloadConfig => current,
            Self::Shutdown => RuntimeState::Shutdown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlPlaneRequest {
    pub command: OperatorCommand,
    pub submitted_by: String,
    pub submitted_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlPlaneDecision {
    pub accepted: bool,
    pub message: String,
    pub resulting_state: RuntimeState,
    pub decided_at: Timestamp,
}
