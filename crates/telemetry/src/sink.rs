use anyhow::Result;
use async_trait::async_trait;
use domain::{ExecutionReport, ExecutionStats, FeatureSnapshot, PnlSnapshot, RegimeDecision, RiskDecision, RuntimeSnapshot, SystemHealth};

#[async_trait]
pub trait TelemetrySink: Send + Sync {
    async fn emit_runtime_state(&self, snapshot: &RuntimeSnapshot) -> Result<()>;
    async fn emit_health(&self, health: &SystemHealth) -> Result<()>;
    async fn emit_features(&self, features: &FeatureSnapshot) -> Result<()>;
    async fn emit_regime(&self, regime: &RegimeDecision) -> Result<()>;
    async fn emit_risk_decision(&self, decision: &RiskDecision) -> Result<()>;
    async fn emit_execution_report(&self, report: &ExecutionReport) -> Result<()>;
    async fn emit_execution_stats(&self, stats: &ExecutionStats) -> Result<()>;
    async fn emit_pnl_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()>;
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

    async fn emit_features(&self, features: &FeatureSnapshot) -> Result<()> {
        tracing::info!(
            symbol = %features.symbol,
            spread_bps = %features.spread_bps,
            realized_vol_bps = %features.realized_volatility_bps,
            local_momentum_bps = %features.local_momentum_bps,
            trade_flow_imbalance = %features.trade_flow_imbalance,
            trade_rate_per_sec = %features.trade_rate_per_sec,
            volatility_regime = ?features.volatility_regime,
            "features"
        );
        Ok(())
    }

    async fn emit_regime(&self, regime: &RegimeDecision) -> Result<()> {
        tracing::info!(
            symbol = %regime.symbol,
            regime = ?regime.state,
            confidence = %regime.confidence,
            reason = regime.reason,
            "regime"
        );
        Ok(())
    }

    async fn emit_risk_decision(&self, decision: &RiskDecision) -> Result<()> {
        tracing::info!(action = ?decision.action, reason = decision.reason, "risk decision");
        Ok(())
    }

    async fn emit_execution_report(&self, report: &ExecutionReport) -> Result<()> {
        tracing::info!(
            status = ?report.status,
            client_order_id = report.client_order_id,
            fill_ratio = %report.fill_ratio,
            slippage_bps = ?report.slippage_bps,
            decision_latency_ms = ?report.decision_latency_ms,
            "execution report"
        );
        Ok(())
    }

    async fn emit_execution_stats(&self, stats: &ExecutionStats) -> Result<()> {
        tracing::info!(
            total_submitted = stats.total_submitted,
            reject_rate = %stats.reject_rate,
            avg_fill_ratio = %stats.avg_fill_ratio,
            avg_slippage_bps = %stats.avg_slippage_bps,
            avg_decision_latency_ms = %stats.avg_decision_latency_ms,
            "execution stats"
        );
        Ok(())
    }

    async fn emit_pnl_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()> {
        tracing::info!(
            net_pnl_quote = %snapshot.net_pnl_quote,
            daily_pnl_quote = %snapshot.daily_pnl_quote,
            realized_pnl_quote = %snapshot.realized_pnl_quote,
            unrealized_pnl_quote = %snapshot.unrealized_pnl_quote,
            drawdown_quote = %snapshot.drawdown_quote,
            "pnl snapshot"
        );
        Ok(())
    }
}
