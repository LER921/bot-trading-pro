use anyhow::Result;
use async_trait::async_trait;
use common::now_utc;
use domain::{ControlPlaneDecision, ControlPlaneRequest, RuntimeState};
use std::sync::Arc;
use tokio::sync::RwLock;

#[async_trait]
pub trait ControlPlane: Send + Sync {
    async fn submit(&self, request: ControlPlaneRequest) -> Result<ControlPlaneDecision>;
    async fn current_state(&self) -> RuntimeState;
}

#[derive(Debug, Clone)]
pub struct InMemoryControlPlane {
    state: Arc<RwLock<RuntimeState>>,
}

impl InMemoryControlPlane {
    pub fn new(initial_state: RuntimeState) -> Self {
        Self {
            state: Arc::new(RwLock::new(initial_state)),
        }
    }
}

#[async_trait]
impl ControlPlane for InMemoryControlPlane {
    async fn submit(&self, request: ControlPlaneRequest) -> Result<ControlPlaneDecision> {
        let mut state = self.state.write().await;
        let next_state = request.command.target_state(*state);
        *state = next_state;

        Ok(ControlPlaneDecision {
            accepted: true,
            message: "control plane accepted request; production wiring must still route through risk".to_string(),
            resulting_state: next_state,
            decided_at: now_utc(),
        })
    }

    async fn current_state(&self) -> RuntimeState {
        *self.state.read().await
    }
}
