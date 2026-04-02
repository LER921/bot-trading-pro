use anyhow::Result;
use async_trait::async_trait;
use domain::{ExecutionReport, FillEvent, RiskDecision, RuntimeTransition};
use std::{sync::Arc, vec::Vec};
use tokio::sync::RwLock;

#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn persist_runtime_transition(&self, transition: &RuntimeTransition) -> Result<()>;
    async fn persist_risk_decision(&self, decision: &RiskDecision) -> Result<()>;
    async fn persist_execution_report(&self, report: &ExecutionReport) -> Result<()>;
    async fn persist_fill(&self, fill: &FillEvent) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}

#[derive(Debug, Default, Clone)]
pub struct MemoryStorage {
    events: Arc<RwLock<Vec<String>>>,
}

#[async_trait]
impl StorageEngine for MemoryStorage {
    async fn persist_runtime_transition(&self, transition: &RuntimeTransition) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("runtime {:?}->{:?}", transition.from, transition.to));
        Ok(())
    }

    async fn persist_risk_decision(&self, decision: &RiskDecision) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("risk {:?} {}", decision.action, decision.reason));
        Ok(())
    }

    async fn persist_execution_report(&self, report: &ExecutionReport) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("execution {:?} {}", report.status, report.client_order_id));
        Ok(())
    }

    async fn persist_fill(&self, fill: &FillEvent) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("fill {} {}", fill.symbol, fill.trade_id));
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
