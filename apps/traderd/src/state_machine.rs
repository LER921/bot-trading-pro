use anyhow::{bail, Result};
use common::now_utc;
use domain::{RuntimeSnapshot, RuntimeState, RuntimeTransition};

#[derive(Debug, Clone)]
pub struct RuntimeStateMachine {
    current: RuntimeSnapshot,
    transitions: Vec<RuntimeTransition>,
}

impl RuntimeStateMachine {
    pub fn new(initial_state: RuntimeState, reason: impl Into<String>) -> Self {
        Self {
            current: RuntimeSnapshot {
                state: initial_state,
                entered_at: now_utc(),
                reason: reason.into(),
            },
            transitions: Vec::new(),
        }
    }

    pub fn current_state(&self) -> RuntimeState {
        self.current.state
    }

    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.current.clone()
    }

    pub fn transitions(&self) -> &[RuntimeTransition] {
        &self.transitions
    }

    pub fn transition(
        &mut self,
        next: RuntimeState,
        reason: impl Into<String>,
    ) -> Result<Option<RuntimeTransition>> {
        if self.current.state == next {
            return Ok(None);
        }

        if !can_transition(self.current.state, next) {
            bail!("invalid runtime transition {:?} -> {:?}", self.current.state, next);
        }

        let reason = reason.into();
        let transition = RuntimeTransition {
            from: self.current.state,
            to: next,
            reason: reason.clone(),
            transitioned_at: now_utc(),
        };

        self.current = RuntimeSnapshot {
            state: next,
            entered_at: transition.transitioned_at,
            reason,
        };
        self.transitions.push(transition.clone());
        Ok(Some(transition))
    }
}

pub fn can_transition(from: RuntimeState, to: RuntimeState) -> bool {
    match from {
        RuntimeState::Bootstrap => matches!(
            to,
            RuntimeState::Reconciling | RuntimeState::Paused | RuntimeState::RiskOff | RuntimeState::Shutdown
        ),
        RuntimeState::Reconciling => matches!(
            to,
            RuntimeState::Ready | RuntimeState::Paused | RuntimeState::RiskOff | RuntimeState::Shutdown
        ),
        RuntimeState::Ready => matches!(
            to,
            RuntimeState::Trading
                | RuntimeState::Reduced
                | RuntimeState::Paused
                | RuntimeState::RiskOff
                | RuntimeState::Reconciling
                | RuntimeState::Shutdown
        ),
        RuntimeState::Trading => matches!(
            to,
            RuntimeState::Reduced
                | RuntimeState::Paused
                | RuntimeState::RiskOff
                | RuntimeState::Reconciling
                | RuntimeState::Shutdown
        ),
        RuntimeState::Reduced => matches!(
            to,
            RuntimeState::Trading
                | RuntimeState::Paused
                | RuntimeState::RiskOff
                | RuntimeState::Reconciling
                | RuntimeState::Shutdown
        ),
        RuntimeState::Paused => matches!(
            to,
            RuntimeState::Ready
                | RuntimeState::Trading
                | RuntimeState::Reduced
                | RuntimeState::RiskOff
                | RuntimeState::Reconciling
                | RuntimeState::Shutdown
        ),
        RuntimeState::RiskOff => matches!(
            to,
            RuntimeState::Paused | RuntimeState::Reconciling | RuntimeState::Shutdown
        ),
        RuntimeState::Shutdown => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_expected_runtime_transition_sequence() {
        let mut machine = RuntimeStateMachine::new(RuntimeState::Bootstrap, "start");
        assert!(machine
            .transition(RuntimeState::Reconciling, "bootstrap complete")
            .unwrap()
            .is_some());
        assert!(machine
            .transition(RuntimeState::Ready, "reconciled")
            .unwrap()
            .is_some());
        assert!(machine
            .transition(RuntimeState::Trading, "live enabled")
            .unwrap()
            .is_some());
    }

    #[test]
    fn rejects_invalid_backward_transition() {
        let mut machine = RuntimeStateMachine::new(RuntimeState::Trading, "live");
        let error = machine
            .transition(RuntimeState::Bootstrap, "invalid")
            .expect_err("transition should fail");
        assert!(error.to_string().contains("invalid runtime transition"));
    }
}
