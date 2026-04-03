use anyhow::Result;
use async_trait::async_trait;
use common::Decimal;
use domain::{
    ExecutionReport, ExecutionStats, FeatureSnapshot, HealthState, PnlSnapshot,
    RegimeDecision, RiskAction, RiskDecision, RuntimeSnapshot, StrategyContext,
    StrategyOutcome, SystemHealth,
};

#[async_trait]
pub trait TelemetrySink: Send + Sync {
    async fn emit_runtime_state(&self, snapshot: &RuntimeSnapshot) -> Result<()>;
    async fn emit_health(&self, health: &SystemHealth) -> Result<()>;
    async fn emit_features(&self, features: &FeatureSnapshot) -> Result<()>;
    async fn emit_regime(&self, regime: &RegimeDecision) -> Result<()>;
    async fn emit_strategy_outcome(
        &self,
        strategy: &str,
        context: &StrategyContext,
        outcome: &StrategyOutcome,
    ) -> Result<()>;
    async fn emit_risk_decision(&self, decision: &RiskDecision) -> Result<()>;
    async fn emit_execution_report(&self, report: &ExecutionReport) -> Result<()>;
    async fn emit_execution_stats(&self, stats: &ExecutionStats) -> Result<()>;
    async fn emit_pnl_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()>;
}

#[derive(Debug, Default, Clone)]
pub struct TracingTelemetry;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeStateDiagnostic {
    lifecycle_stage: &'static str,
    trading_allowed: bool,
    reason_tags: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HealthDiagnostic {
    degraded_components: Vec<&'static str>,
    primary_cause: &'static str,
    diagnosis: String,
    trading_blocked: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct StrategyOutcomeDiagnostic {
    status: &'static str,
    reason: String,
    reason_tags: Vec<&'static str>,
    intent_count: usize,
    best_edge_after_cost_bps: Decimal,
    reduce_only_intents: usize,
    sample_reason: String,
}

#[derive(Debug, Clone, PartialEq)]
struct RiskDecisionDiagnostic {
    disposition: &'static str,
    reason_tags: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq)]
struct ExecutionStatsDiagnostic {
    fill_report_rate: Decimal,
    cancel_rate: Decimal,
    stale_cancel_share: Decimal,
    non_stale_cancels: u64,
    duplicate_suppressions: u64,
    churn_state: &'static str,
}

#[async_trait]
impl TelemetrySink for TracingTelemetry {
    async fn emit_runtime_state(&self, snapshot: &RuntimeSnapshot) -> Result<()> {
        let diagnostic = runtime_state_diagnostic(snapshot);
        tracing::info!(
            state = ?snapshot.state,
            entered_at = ?snapshot.entered_at,
            reason = snapshot.reason,
            lifecycle_stage = diagnostic.lifecycle_stage,
            trading_allowed = diagnostic.trading_allowed,
            reason_tags = %join_fields(&diagnostic.reason_tags),
            "runtime state"
        );
        Ok(())
    }

    async fn emit_health(&self, health: &SystemHealth) -> Result<()> {
        let diagnostic = health_diagnostic(health);
        tracing::info!(
            overall_state = ?health.overall_state,
            market_ws = ?health.market_ws.state,
            user_ws = ?health.user_ws.state,
            rest = ?health.rest.state,
            clock_drift = ?health.clock_drift.state,
            market_data = ?health.market_data.state,
            account_events = ?health.account_events.state,
            fallback_active = health.fallback_active,
            degraded_components = %join_fields(&diagnostic.degraded_components),
            primary_cause = diagnostic.primary_cause,
            trading_blocked = diagnostic.trading_blocked,
            diagnosis = diagnostic.diagnosis,
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
            momentum_1s_bps = %features.momentum_1s_bps,
            trade_flow_imbalance = %features.trade_flow_imbalance,
            orderbook_imbalance = ?features.orderbook_imbalance,
            vwap_distance_bps = %features.vwap_distance_bps,
            toxicity_score = %features.toxicity_score,
            flow_microstructure = %format!(
                "f250={} f500={} f1s={} drift500={} drift1s={} instab={}",
                features.microstructure.flow.trade_flow_imbalance_250ms,
                features.microstructure.flow.trade_flow_imbalance_500ms,
                features.microstructure.flow.trade_flow_imbalance_1s,
                features.microstructure.book.micro_mid_drift_500ms_bps,
                features.microstructure.book.micro_mid_drift_1s_bps,
                features.microstructure.book.book_instability_score
            ),
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

    async fn emit_strategy_outcome(
        &self,
        strategy: &str,
        context: &StrategyContext,
        outcome: &StrategyOutcome,
    ) -> Result<()> {
        let diagnostic = strategy_outcome_diagnostic(strategy, outcome);
        let selected_setups = join_fields(
            &outcome
                .intents
                .iter()
                .filter_map(|intent| intent.setup_type.as_deref())
                .collect::<Vec<_>>(),
        );
        let size_tiers = join_fields(
            &outcome
                .intents
                .iter()
                .filter_map(|intent| intent.size_tier.as_deref())
                .collect::<Vec<_>>(),
        );
        tracing::info!(
            symbol = %context.symbol,
            strategy = strategy,
            status = diagnostic.status,
            standby_reason = diagnostic.reason,
            reason_tags = %join_fields(&diagnostic.reason_tags),
            intent_count = diagnostic.intent_count,
            best_edge_after_cost_bps = %diagnostic.best_edge_after_cost_bps,
            reduce_only_intents = diagnostic.reduce_only_intents,
            sample_reason = diagnostic.sample_reason,
            runtime_state = ?context.runtime_state,
            risk_mode = ?context.risk_mode,
            inventory_base = %context.inventory.base_position,
            spread_bps = %context.features.spread_bps,
            toxicity_score = %context.features.toxicity_score,
            local_momentum_bps = %context.features.local_momentum_bps,
            trade_flow_imbalance = %context.features.trade_flow_imbalance,
            orderbook_imbalance = ?context.features.orderbook_imbalance,
            fill_quality_score = %context.fill_quality.fill_quality_score,
            adverse_selection_rate = %context.fill_quality.adverse_selection_rate,
            best_expected_realized_edge_bps = ?outcome.best_expected_realized_edge_bps,
            entry_block_reason = ?outcome.entry_block_reason,
            selected_setups = selected_setups,
            size_tiers = size_tiers,
            "strategy outcome"
        );
        Ok(())
    }

    async fn emit_risk_decision(&self, decision: &RiskDecision) -> Result<()> {
        let diagnostic = risk_decision_diagnostic(decision);
        let approved_quantity = decision.approved_intent.as_ref().map(|intent| intent.quantity);
        let approved_edge_after_cost_bps = decision
            .approved_intent
            .as_ref()
            .map(|intent| intent.edge_after_cost_bps);
        let approved_reduce_only = decision
            .approved_intent
            .as_ref()
            .map(|intent| intent.reduce_only)
            .unwrap_or(false);

        tracing::info!(
            action = ?decision.action,
            disposition = diagnostic.disposition,
            resulting_mode = ?decision.resulting_mode,
            symbol = %decision.original_intent.symbol,
            strategy = ?decision.original_intent.strategy,
            side = ?decision.original_intent.side,
            requested_quantity = %decision.original_intent.quantity,
            requested_edge_after_cost_bps = %decision.original_intent.edge_after_cost_bps,
            approved_quantity = ?approved_quantity,
            approved_edge_after_cost_bps = ?approved_edge_after_cost_bps,
            approved_reduce_only = approved_reduce_only,
            reason = decision.reason,
            reason_tags = %join_fields(&diagnostic.reason_tags),
            "risk decision"
        );
        Ok(())
    }

    async fn emit_execution_report(&self, report: &ExecutionReport) -> Result<()> {
        tracing::info!(
            symbol = %report.symbol,
            status = ?report.status,
            client_order_id = report.client_order_id,
            requested_price = ?report.requested_price,
            average_fill_price = ?report.average_fill_price,
            fill_ratio = %report.fill_ratio,
            slippage_bps = ?report.slippage_bps,
            decision_latency_ms = ?report.decision_latency_ms,
            submit_ack_latency_ms = ?report.submit_ack_latency_ms,
            submit_to_first_report_ms = ?report.submit_to_first_report_ms,
            submit_to_fill_ms = ?report.submit_to_fill_ms,
            exchange_order_age_ms = ?report.exchange_order_age_ms,
            expected_realized_edge_bps = ?report.expected_realized_edge_bps,
            adverse_selection_penalty_bps = ?report.adverse_selection_penalty_bps,
            setup_type = ?report.setup_type,
            size_tier = ?report.size_tier,
            message = report.message.as_deref().unwrap_or(""),
            "execution report"
        );
        Ok(())
    }

    async fn emit_execution_stats(&self, stats: &ExecutionStats) -> Result<()> {
        let diagnostic = execution_stats_diagnostic(stats);
        tracing::info!(
            total_submitted = stats.total_submitted,
            total_rejected = stats.total_rejected,
            total_local_validation_rejects = stats.total_local_validation_rejects,
            total_benign_exchange_rejected = stats.total_benign_exchange_rejected,
            risk_scored_rejections = stats.risk_scored_rejections,
            total_canceled = stats.total_canceled,
            total_manual_cancels = stats.total_manual_cancels,
            total_external_cancels = stats.total_external_cancels,
            total_stale_cancels = stats.total_stale_cancels,
            total_filled_reports = stats.total_filled_reports,
            total_duplicate_intents = stats.total_duplicate_intents,
            total_equivalent_order_skips = stats.total_equivalent_order_skips,
            duplicate_suppressions = diagnostic.duplicate_suppressions,
            non_stale_cancels = diagnostic.non_stale_cancels,
            reject_rate = %stats.reject_rate,
            risk_reject_rate = %stats.risk_reject_rate,
            fill_report_rate = %diagnostic.fill_report_rate,
            cancel_rate = %diagnostic.cancel_rate,
            stale_cancel_share = %diagnostic.stale_cancel_share,
            churn_state = diagnostic.churn_state,
            avg_fill_ratio = %stats.avg_fill_ratio,
            avg_slippage_bps = %stats.avg_slippage_bps,
            avg_decision_latency_ms = %stats.avg_decision_latency_ms,
            avg_submit_ack_latency_ms = %stats.avg_submit_ack_latency_ms,
            avg_submit_to_first_report_ms = %stats.avg_submit_to_first_report_ms,
            avg_submit_to_fill_ms = %stats.avg_submit_to_fill_ms,
            avg_cancel_ack_latency_ms = %stats.avg_cancel_ack_latency_ms,
            decision_latency_samples = stats.decision_latency_samples,
            submit_ack_latency_samples = stats.submit_ack_latency_samples,
            submit_to_first_report_samples = stats.submit_to_first_report_samples,
            submit_to_fill_samples = stats.submit_to_fill_samples,
            cancel_ack_latency_samples = stats.cancel_ack_latency_samples,
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

fn runtime_state_diagnostic(snapshot: &RuntimeSnapshot) -> RuntimeStateDiagnostic {
    let lifecycle_stage = match snapshot.state {
        domain::RuntimeState::Bootstrap => "bootstrap",
        domain::RuntimeState::Reconciling => "recovery",
        domain::RuntimeState::Ready => "ready",
        domain::RuntimeState::Trading => "trading",
        domain::RuntimeState::Reduced => "degraded",
        domain::RuntimeState::Paused => "operator_hold",
        domain::RuntimeState::RiskOff => "blocked",
        domain::RuntimeState::Shutdown => "shutdown",
    };

    RuntimeStateDiagnostic {
        lifecycle_stage,
        trading_allowed: snapshot.state.allows_new_orders(),
        reason_tags: reason_tags(&snapshot.reason),
    }
}

fn health_diagnostic(health: &SystemHealth) -> HealthDiagnostic {
    let mut degraded_components = Vec::new();
    collect_component(&mut degraded_components, "market_ws", health.market_ws.state);
    collect_component(&mut degraded_components, "user_ws", health.user_ws.state);
    collect_component(&mut degraded_components, "rest", health.rest.state);
    collect_component(&mut degraded_components, "clock_drift", health.clock_drift.state);
    collect_component(&mut degraded_components, "market_data", health.market_data.state);
    collect_component(
        &mut degraded_components,
        "account_events",
        health.account_events.state,
    );

    let primary_cause = if health.market_data.state == HealthState::Stale {
        "market_data_stale"
    } else if health.clock_drift.state == HealthState::Unhealthy {
        "clock_drift_unhealthy"
    } else if health.rest.state == HealthState::Unhealthy {
        "rest_unhealthy"
    } else if health.market_ws.state != HealthState::Healthy {
        "market_ws_degraded"
    } else if health.user_ws.state != HealthState::Healthy {
        "user_ws_degraded"
    } else if health.account_events.state != HealthState::Healthy {
        "account_events_degraded"
    } else if health.clock_drift.state == HealthState::Degraded {
        "clock_drift_degraded"
    } else {
        "healthy"
    };

    let diagnosis = if degraded_components.is_empty() {
        "all runtime health checks healthy".to_string()
    } else if health.fallback_active {
        format!(
            "rest fallback active while health is degraded: {}",
            join_fields(&degraded_components)
        )
    } else {
        format!("health degraded: {}", join_fields(&degraded_components))
    };

    HealthDiagnostic {
        degraded_components,
        primary_cause,
        diagnosis,
        trading_blocked: matches!(health.overall_state, HealthState::Unhealthy | HealthState::Stale),
    }
}

fn strategy_outcome_diagnostic(strategy: &str, outcome: &StrategyOutcome) -> StrategyOutcomeDiagnostic {
    let best_edge_after_cost_bps = outcome
        .intents
        .iter()
        .map(|intent| intent.edge_after_cost_bps)
        .max()
        .unwrap_or(Decimal::ZERO);
    let reduce_only_intents = outcome
        .intents
        .iter()
        .filter(|intent| intent.reduce_only)
        .count();
    let reason = outcome
        .standby_reason
        .clone()
        .unwrap_or_else(|| format!("{strategy} produced actionable intents"));
    let sample_reason = outcome
        .intents
        .first()
        .map(|intent| intent.reason.clone())
        .unwrap_or_else(|| reason.clone());

    StrategyOutcomeDiagnostic {
        status: if outcome.intents.is_empty() {
            "standby"
        } else {
            "actionable"
        },
        reason_tags: reason_tags(&reason),
        reason,
        intent_count: outcome.intents.len(),
        best_edge_after_cost_bps,
        reduce_only_intents,
        sample_reason,
    }
}

fn risk_decision_diagnostic(decision: &RiskDecision) -> RiskDecisionDiagnostic {
    let disposition = match decision.action {
        RiskAction::Approve => "approved",
        RiskAction::Resize => "resized",
        RiskAction::Block => "blocked",
        RiskAction::ReduceOnly => "reduce_only",
        RiskAction::CancelAll => "cancel_all",
    };

    RiskDecisionDiagnostic {
        disposition,
        reason_tags: reason_tags(&decision.reason),
    }
}

fn execution_stats_diagnostic(stats: &ExecutionStats) -> ExecutionStatsDiagnostic {
    let fill_report_rate = ratio(stats.total_filled_reports, stats.total_submitted);
    let cancel_rate = ratio(stats.total_canceled, stats.total_submitted);
    let stale_cancel_share = ratio(stats.total_stale_cancels, stats.total_canceled);
    let non_stale_cancels = stats.total_canceled.saturating_sub(stats.total_stale_cancels);
    let duplicate_suppressions =
        stats.total_duplicate_intents + stats.total_equivalent_order_skips;

    let churn_state = if stats.total_submitted == 0 {
        "idle"
    } else if cancel_rate >= dec("0.75") && fill_report_rate <= dec("0.25") {
        "high_churn"
    } else if fill_report_rate >= dec("0.50") && cancel_rate <= dec("0.35") {
        "efficient"
    } else {
        "mixed"
    };

    ExecutionStatsDiagnostic {
        fill_report_rate,
        cancel_rate,
        stale_cancel_share,
        non_stale_cancels,
        duplicate_suppressions,
        churn_state,
    }
}

fn collect_component(components: &mut Vec<&'static str>, name: &'static str, state: HealthState) {
    if state != HealthState::Healthy {
        components.push(name);
    }
}

fn reason_tags(reason: &str) -> Vec<&'static str> {
    let reason = reason.to_ascii_lowercase();
    let mut tags = Vec::new();

    push_tag(
        &mut tags,
        "edge_costs",
        ["edge", "cost", "spread too tight", "after costs"].as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "toxicity",
        ["tox", "toxic"].as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "inventory",
        ["inventory", "position cap", "reduce-only", "reduce only", "position"]
            .as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "regime",
        ["regime"].as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "flow_microstructure",
        [
            "momentum",
            "flow",
            "book",
            "pressure",
            "direction",
            "vwap",
            "extension",
            "invalidation",
        ]
        .as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "market_data",
        ["liquidity", "best bid/ask", "best bid", "orderbook", "market data"].as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "recovery",
        ["recovery", "reconcil", "pending fill", "resync", "bootstrap"].as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "operator",
        ["operator", "pause", "resume", "shutdown"].as_slice(),
        &reason,
    );
    push_tag(
        &mut tags,
        "runtime_health",
        [
            "risk-off",
            "risk off",
            "hard health block",
            "clock drift",
            "stale",
            "websocket",
            "fallback",
        ]
        .as_slice(),
        &reason,
    );

    if tags.is_empty() {
        tags.push("unspecified");
    }

    tags
}

fn push_tag(tags: &mut Vec<&'static str>, tag: &'static str, needles: &[&str], haystack: &str) {
    if needles.iter().any(|needle| haystack.contains(needle)) && !tags.contains(&tag) {
        tags.push(tag);
    }
}

fn ratio(numerator: u64, denominator: u64) -> Decimal {
    if denominator == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(numerator) / Decimal::from(denominator)
    }
}

fn join_fields(fields: &[&str]) -> String {
    if fields.is_empty() {
        "none".to_string()
    } else {
        fields.join("|")
    }
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::now_utc;
    use domain::{
        BestBidAsk, InventorySnapshot, RegimeDecision, RegimeState, RiskMode, RuntimeState,
        Side, StrategyKind, Symbol, TradeIntent, VolatilityRegime,
    };

    fn sample_context() -> StrategyContext {
        StrategyContext {
            symbol: Symbol::BtcUsdc,
            best_bid_ask: Some(BestBidAsk {
                symbol: Symbol::BtcUsdc,
                bid_price: dec("100.00"),
                bid_quantity: dec("1.0"),
                ask_price: dec("100.02"),
                ask_quantity: dec("1.0"),
                observed_at: now_utc(),
            }),
            features: FeatureSnapshot {
                symbol: Symbol::BtcUsdc,
                microprice: Some(dec("100.01")),
                top_of_book_depth_quote: dec("4000"),
                orderbook_imbalance: Some(dec("0.10")),
                orderbook_imbalance_rolling: dec("0.08"),
                realized_volatility_bps: dec("3.0"),
                vwap: dec("99.99"),
                vwap_distance_bps: dec("1.5"),
                spread_bps: dec("3.0"),
                spread_mean_bps: dec("2.5"),
                spread_std_bps: dec("0.4"),
                spread_zscore: dec("0.20"),
                trade_flow_imbalance: dec("0.15"),
                trade_flow_rate: dec("1.0"),
                trade_rate_per_sec: dec("1.2"),
                tick_rate_per_sec: dec("2.0"),
                tape_speed: dec("1.0"),
                momentum_1s_bps: dec("4.0"),
                momentum_5s_bps: dec("3.0"),
                momentum_15s_bps: dec("2.0"),
                local_momentum_bps: dec("5.0"),
                liquidity_score: dec("5000"),
                toxicity_score: dec("0.20"),
                microstructure: MicrostructureSnapshot::default(),
                volatility_regime: VolatilityRegime::Low,
                computed_at: now_utc(),
            },
            fill_quality: FillQualitySnapshot::default(),
            regime: RegimeDecision {
                symbol: Symbol::BtcUsdc,
                state: RegimeState::Range,
                confidence: dec("0.80"),
                reason: "range".to_string(),
                decided_at: now_utc(),
            },
            inventory: InventorySnapshot {
                symbol: Symbol::BtcUsdc,
                base_position: dec("0.010"),
                quote_position: dec("-100"),
                mark_price: Some(dec("100.01")),
                average_entry_price: Some(dec("99.95")),
                position_opened_at: Some(now_utc()),
                last_fill_at: Some(now_utc()),
                first_reduce_at: None,
                updated_at: now_utc(),
            },
            soft_inventory_base: dec("0.020"),
            max_inventory_base: dec("0.050"),
            local_min_notional_quote: dec("10.00"),
            open_bot_orders_for_symbol: 1,
            max_open_orders_for_symbol: 4,
            runtime_state: RuntimeState::Trading,
            risk_mode: RiskMode::Normal,
        }
    }

    fn sample_intent() -> TradeIntent {
        TradeIntent {
            intent_id: "intent-1".to_string(),
            symbol: Symbol::BtcUsdc,
            strategy: StrategyKind::MarketMaking,
            side: Side::Buy,
            quantity: dec("0.005"),
            limit_price: Some(dec("100.00")),
            max_slippage_bps: dec("2.0"),
            post_only: true,
            reduce_only: false,
            time_in_force: Some(domain::TimeInForce::Gtc),
            role: domain::IntentRole::AddRisk,
            exit_stage: None,
            exit_reason: None,
            expected_edge_bps: dec("3.0"),
            expected_fee_bps: dec("0.75"),
            expected_slippage_bps: dec("0.25"),
            edge_after_cost_bps: dec("2.0"),
            expected_realized_edge_bps: dec("1.8"),
            adverse_selection_penalty_bps: dec("0.2"),
            setup_type: Some("passive_long_favorable".to_string()),
            size_tier: Some("normal_size".to_string()),
            reason: "mm bid gated by edge after costs recovered".to_string(),
            created_at: now_utc(),
            expires_at: Some(now_utc()),
        }
    }

    #[test]
    fn strategy_outcome_diagnostic_propagates_standby_reason_and_tags() {
        let outcome = StrategyOutcome {
            intents: Vec::new(),
            standby_reason: Some("scalping blocked by toxicity".to_string()),
        };

        let diagnostic = strategy_outcome_diagnostic("scalping", &outcome);

        assert_eq!(diagnostic.status, "standby");
        assert_eq!(diagnostic.reason, "scalping blocked by toxicity");
        assert!(diagnostic.reason_tags.contains(&"toxicity"));
    }

    #[test]
    fn runtime_and_health_diagnostics_expose_runtime_blockers() {
        let snapshot = RuntimeSnapshot {
            state: RuntimeState::Reconciling,
            entered_at: now_utc(),
            reason: "runtime recovery gate active: pending fill recoveries waiting on REST".to_string(),
        };
        let runtime = runtime_state_diagnostic(&snapshot);
        assert_eq!(runtime.lifecycle_stage, "recovery");
        assert!(runtime.reason_tags.contains(&"recovery"));

        let health = SystemHealth {
            overall_state: HealthState::Degraded,
            market_ws: domain::MarketStreamHealth {
                state: HealthState::Degraded,
                last_message_at: Some(now_utc()),
                last_event_age_ms: Some(1_500),
                reconnect_count: 1,
            },
            user_ws: domain::UserStreamHealth {
                state: HealthState::Healthy,
                last_message_at: Some(now_utc()),
                last_event_age_ms: Some(0),
                reconnect_count: 0,
            },
            rest: domain::RestHealth {
                state: HealthState::Healthy,
                last_success_at: Some(now_utc()),
                consecutive_failures: 0,
            },
            clock_drift: domain::ClockDriftHealth {
                state: HealthState::Healthy,
                drift_ms: 10,
                sampled_at: now_utc(),
            },
            market_data: domain::MarketDataFreshness {
                state: HealthState::Healthy,
                last_orderbook_update_at: Some(now_utc()),
                last_trade_update_at: Some(now_utc()),
            },
            account_events: domain::AccountEventFreshness {
                state: HealthState::Degraded,
                last_balance_event_at: Some(now_utc()),
                last_fill_event_at: Some(now_utc()),
            },
            fallback_active: true,
            updated_at: now_utc(),
        };

        let diagnostic = health_diagnostic(&health);
        assert_eq!(diagnostic.primary_cause, "market_ws_degraded");
        assert!(diagnostic.degraded_components.contains(&"market_ws"));
        assert!(diagnostic.degraded_components.contains(&"account_events"));
        assert!(diagnostic.diagnosis.contains("fallback"));
    }

    #[test]
    fn risk_decision_diagnostic_keeps_refusal_cause_visible() {
        let decision = RiskDecision {
            action: RiskAction::Block,
            original_intent: sample_intent(),
            approved_intent: None,
            reason: "inventory imbalance allows only reduce-only orders".to_string(),
            resulting_mode: RiskMode::ReduceOnly,
            decided_at: now_utc(),
        };

        let diagnostic = risk_decision_diagnostic(&decision);

        assert_eq!(diagnostic.disposition, "blocked");
        assert!(diagnostic.reason_tags.contains(&"inventory"));
    }

    #[test]
    fn execution_stats_diagnostic_surfaces_churn_and_duplicate_suppression() {
        let stats = ExecutionStats {
            total_submitted: 10,
            total_rejected: 1,
            total_local_validation_rejects: 2,
            total_benign_exchange_rejected: 1,
            risk_scored_rejections: 1,
            total_canceled: 8,
            total_manual_cancels: 3,
            total_external_cancels: 1,
            total_filled_reports: 2,
            total_stale_cancels: 5,
            total_duplicate_intents: 2,
            total_equivalent_order_skips: 1,
            reject_rate: dec("0.10"),
            risk_reject_rate: dec("0.10"),
            avg_fill_ratio: dec("0.30"),
            avg_slippage_bps: dec("0.20"),
            avg_decision_latency_ms: dec("12"),
            updated_at: now_utc(),
        };

        let diagnostic = execution_stats_diagnostic(&stats);

        assert_eq!(diagnostic.fill_report_rate, dec("0.2"));
        assert_eq!(diagnostic.cancel_rate, dec("0.8"));
        assert_eq!(diagnostic.stale_cancel_share, dec("0.625"));
        assert_eq!(diagnostic.non_stale_cancels, 3);
        assert_eq!(diagnostic.duplicate_suppressions, 3);
        assert_eq!(diagnostic.churn_state, "high_churn");
    }

    #[test]
    fn strategy_outcome_diagnostic_preserves_actionable_intent_context() {
        let context = sample_context();
        let outcome = StrategyOutcome {
            intents: vec![sample_intent()],
            standby_reason: None,
        };

        let diagnostic = strategy_outcome_diagnostic("market_making", &outcome);

        assert_eq!(diagnostic.status, "actionable");
        assert_eq!(diagnostic.intent_count, 1);
        assert_eq!(diagnostic.best_edge_after_cost_bps, dec("2.0"));
        assert!(diagnostic.sample_reason.contains("edge after costs"));

        let health = health_diagnostic(&SystemHealth::healthy(context.features.computed_at));
        assert_eq!(health.primary_cause, "healthy");
    }
}
