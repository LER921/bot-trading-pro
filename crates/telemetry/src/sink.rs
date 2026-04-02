use anyhow::Result;
use async_trait::async_trait;
use domain::{ExecutionReport, RiskDecision, RuntimeSnapshot, SystemHealth};

#[async_trait]
pub trait TelemetrySink: Send + Sync {
    async fn emit_runtime_state(&self, snapshot: &RuntimeSnapshot) -> Result<()>;
    async fn emit_health(&self, health: &SystemHealth) -> Result<()>;
    async fn emit_risk_decision(&self, decision: &RiskDecision) -> Result<()>;
    async fn emit_execution_report(&self, report: &ExecutionReport) -> Result<()>;
}

#[derive(Debug, Default, Clone)]
pub struct TracingTelemetry;

#[async_trait]
impl TelemetrySink for TracingTelemetry {
    async fn emit_runtime_state(&self, snapshot: &RuntimeSnapshot) -> Result<()> {
        tracing::info!(state = ?snapshot.state, reason = snapshot.reason, "runtime state");
        Ok(())
    }

    async fn emit_health(&self, health: &SystemHealth) -> Result<()> {
        tracing::info!(
            market_ws = ?health.market_ws.state,
            user_ws = ?health.user_ws.state,
            rest = ?health.rest.state,
            market_data = ?health.market_data.state,
            account_events = ?health.account_events.state,
            fallback_active = health.fallback_active,
            "system health"
        );
        Ok(())
    }

    async fn emit_risk_decision(&self, decision: &RiskDecision) -> Result<()> {
        tracing::info!(action = ?decision.action, reason = decision.reason, "risk decision");
        Ok(())
    }

    async fn emit_execution_report(&self, report: &ExecutionReport) -> Result<()> {
        tracing::info!(status = ?report.status, client_order_id = report.client_order_id, "execution report");
        Ok(())
    }
}
