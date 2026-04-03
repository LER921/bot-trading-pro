use common::{Decimal, ids::new_id, now_utc};
use domain::{
    BestBidAsk, ExitStage, IntentRole, RegimeState, Side, StrategyContext, StrategyKind,
    StrategyOutcome, TimeInForce, TradeIntent, VolatilityRegime,
};

#[derive(Debug, Clone)]
pub struct ScalpingConfig {
    pub taker_fee_bps: Decimal,
    pub slippage_buffer_bps: Decimal,
    pub min_entry_score: Decimal,
    pub min_momentum_bps: Decimal,
    pub max_toxicity_score: Decimal,
    pub max_position_fraction: Decimal,
    pub quote_size_fraction: Decimal,
    pub quote_ttl_secs: i64,
    pub soft_hold_secs: i64,
    pub stale_hold_secs: i64,
    pub aggressive_exit_secs: i64,
}

impl Default for ScalpingConfig {
    fn default() -> Self {
        Self {
            taker_fee_bps: Decimal::from_str_exact("1.00").unwrap(),
            slippage_buffer_bps: Decimal::from_str_exact("0.50").unwrap(),
            min_entry_score: Decimal::from(6u32),
            min_momentum_bps: Decimal::from(4u32),
            max_toxicity_score: Decimal::from_str_exact("0.85").unwrap(),
            max_position_fraction: Decimal::from_str_exact("0.50").unwrap(),
            quote_size_fraction: Decimal::from_str_exact("0.10").unwrap(),
            quote_ttl_secs: 2,
            soft_hold_secs: 6,
            stale_hold_secs: 14,
            aggressive_exit_secs: 24,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScalpingStrategy {
    pub config: ScalpingConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntrySetup {
    PassiveLongFavorable,
    AbsorptionReversalLong,
    ProbeEntry,
}

impl EntrySetup {
    fn as_str(self) -> &'static str {
        match self {
            Self::PassiveLongFavorable => "passive_long_favorable",
            Self::AbsorptionReversalLong => "absorption_reversal_long",
            Self::ProbeEntry => "probe_entry",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SizeTier {
    Zero,
    Probe,
    Normal,
    Strong,
}

impl SizeTier {
    fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero_size",
            Self::Probe => "probe_size",
            Self::Normal => "normal_size",
            Self::Strong => "strong_signal_size",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitTrigger {
    MicroTargetHit,
    FlowReversal,
    AdverseMarkout,
    LiquidityCollapse,
    AbsorptionFailure,
    StaleInventory,
}

impl ExitTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::MicroTargetHit => "micro_target_hit",
            Self::FlowReversal => "flow_reversal",
            Self::AdverseMarkout => "adverse_markout",
            Self::LiquidityCollapse => "liquidity_collapse",
            Self::AbsorptionFailure => "absorption_failure",
            Self::StaleInventory => "stale_inventory",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct EntryCandidate {
    setup: EntrySetup,
    expected_edge_bps: Decimal,
    edge_after_cost_bps: Decimal,
    expected_realized_edge_bps: Decimal,
    adverse_selection_penalty_bps: Decimal,
    flow_penalty_bps: Decimal,
    book_instability_penalty_bps: Decimal,
    absorption_bonus_bps: Decimal,
    favorable_markout_bonus_bps: Decimal,
    signal_quality: Decimal,
    post_only: bool,
    time_in_force: TimeInForce,
    limit_price: Decimal,
    probe_mode: bool,
    strong_signal: bool,
}

#[derive(Debug, Clone, Copy)]
struct ExitPlan {
    trigger: ExitTrigger,
    role: IntentRole,
    exit_stage: ExitStage,
    post_only: bool,
    time_in_force: TimeInForce,
    limit_price: Decimal,
    exit_quantity: Decimal,
    pnl_bps: Decimal,
    expected_realized_edge_bps: Decimal,
    adverse_selection_penalty_bps: Decimal,
    position_age_secs: i64,
    target_bps: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct EntryDiagnostics {
    expected_edge_bps: Decimal,
    best_expected_realized_edge_bps: Decimal,
    adverse_selection_penalty_bps: Decimal,
    flow_penalty_bps: Decimal,
    book_instability_penalty_bps: Decimal,
    absorption_bonus_bps: Decimal,
    favorable_markout_bonus_bps: Decimal,
    flow_alignment: Decimal,
    momentum_signal_bps: Decimal,
    ask_fragility: Decimal,
    bid_support: Decimal,
    book_instability: Decimal,
}

impl ScalpingStrategy {
    pub fn evaluate(&self, context: &StrategyContext) -> StrategyOutcome {
        let Some(best) = &context.best_bid_ask else {
            return standby("scalping waiting for best bid/ask");
        };

        if context.features.liquidity_score <= Decimal::ZERO
            || context.features.top_of_book_depth_quote <= Decimal::ZERO
        {
            return blocked(
                "scalping blocked by weak liquidity",
                "liquidity",
                None,
                adverse_hits(context),
            );
        }

        let max_position = context.max_inventory_base * self.config.max_position_fraction;
        let base_quote_size = (context.soft_inventory_base * self.config.quote_size_fraction)
            .max(dec("0.0001"));
        let expires_at = Some(now_utc() + time::Duration::seconds(self.config.quote_ttl_secs));

        if context.inventory.base_position > Decimal::ZERO {
            if let Some(exit) =
                self.exit_intent(context, best, base_quote_size, max_position, expires_at)
            {
                let best_expected_realized_edge_bps = exit.expected_realized_edge_bps;
                return StrategyOutcome {
                    intents: vec![exit],
                    standby_reason: None,
                    entry_block_reason: None,
                    best_expected_realized_edge_bps: Some(best_expected_realized_edge_bps),
                    adverse_selection_hits: adverse_hits(context),
                };
            }
        }

        if !matches!(context.regime.state, RegimeState::TrendUp | RegimeState::Range) {
            return blocked(
                "scalping entry disabled by regime",
                "regime",
                None,
                adverse_hits(context),
            );
        }

        if context.inventory.base_position >= max_position {
            return blocked(
                "scalping blocked by position cap",
                "inventory",
                None,
                adverse_hits(context),
            );
        }

        if available_order_slots(context) == 0 {
            return blocked(
                "scalping blocked by symbol order-slot saturation",
                "slot_pressure",
                None,
                adverse_hits(context),
            );
        }

        let diagnostics = entry_diagnostics(context, &self.config);
        let threshold = expected_realized_threshold(context.symbol);
        let candidates = build_entry_candidates(context, best, &self.config, diagnostics, threshold);

        let Some(candidate) = candidates
            .into_iter()
            .max_by(|left, right| left.expected_realized_edge_bps.cmp(&right.expected_realized_edge_bps))
        else {
            return blocked(
                entry_block_message(context, diagnostics, threshold),
                entry_block_reason(context, diagnostics, threshold),
                Some(diagnostics.best_expected_realized_edge_bps),
                adverse_hits(context),
            );
        };

        let (quantity, size_tier) = entry_size(
            context,
            best,
            base_quote_size,
            max_position,
            candidate,
            threshold,
        );
        if quantity <= Decimal::ZERO {
            return blocked(
                "scalping size collapsed after inventory and signal filters",
                "size_collapsed",
                Some(candidate.expected_realized_edge_bps),
                adverse_hits(context),
            );
        }

        if !has_deployable_notional(
            quantity,
            candidate.limit_price,
            context.local_min_notional_quote,
        ) {
            return blocked(
                "scalping size below local deployable notional floor",
                "local_min_notional",
                Some(candidate.expected_realized_edge_bps),
                adverse_hits(context),
            );
        }

        let setup_type = candidate.setup.as_str().to_string();
        let size_tier_label = size_tier.as_str().to_string();
        let reason = format!(
            "scalp setup={} edge_after_cost={} expected_realized={} adverse_penalty={} flow_penalty={} book_penalty={} absorption_bonus={} markout_bonus={} flow={} mom={} bid_support={} ask_fragility={} instab={} size_tier={} qty={} slots={}/{}",
            setup_type,
            candidate.edge_after_cost_bps,
            candidate.expected_realized_edge_bps,
            candidate.adverse_selection_penalty_bps,
            candidate.flow_penalty_bps,
            candidate.book_instability_penalty_bps,
            candidate.absorption_bonus_bps,
            candidate.favorable_markout_bonus_bps,
            diagnostics.flow_alignment,
            diagnostics.momentum_signal_bps,
            diagnostics.bid_support,
            diagnostics.ask_fragility,
            diagnostics.book_instability,
            size_tier_label,
            quantity,
            context.open_bot_orders_for_symbol,
            context.max_open_orders_for_symbol,
        );

        StrategyOutcome {
            intents: vec![TradeIntent {
                intent_id: format!("scalp-{}", new_id()),
                symbol: context.symbol,
                strategy: StrategyKind::Scalping,
                side: Side::Buy,
                quantity,
                limit_price: Some(candidate.limit_price),
                max_slippage_bps: self.config.slippage_buffer_bps,
                post_only: candidate.post_only,
                reduce_only: false,
                time_in_force: Some(candidate.time_in_force),
                role: IntentRole::AddRisk,
                exit_stage: None,
                exit_reason: None,
                expected_edge_bps: candidate.expected_edge_bps,
                expected_fee_bps: estimated_entry_cost_bps(&self.config, candidate.post_only),
                expected_slippage_bps: if candidate.post_only {
                    self.config.slippage_buffer_bps * dec("0.20")
                } else {
                    self.config.slippage_buffer_bps
                },
                edge_after_cost_bps: candidate.edge_after_cost_bps,
                expected_realized_edge_bps: candidate.expected_realized_edge_bps,
                adverse_selection_penalty_bps: candidate.adverse_selection_penalty_bps,
                setup_type: Some(setup_type),
                size_tier: Some(size_tier_label),
                reason,
                created_at: now_utc(),
                expires_at,
            }],
            standby_reason: None,
            entry_block_reason: None,
            best_expected_realized_edge_bps: Some(candidate.expected_realized_edge_bps),
            adverse_selection_hits: adverse_hits(context),
        }
    }

    fn exit_intent(
        &self,
        context: &StrategyContext,
        best: &BestBidAsk,
        base_quote_size: Decimal,
        max_position: Decimal,
        expires_at: Option<common::Timestamp>,
    ) -> Option<TradeIntent> {
        let average_entry = context.inventory.average_entry_price?;
        if average_entry <= Decimal::ZERO {
            return None;
        }

        let plan =
            analyze_exit(context, &self.config, best, average_entry, base_quote_size, max_position)?;
        let quantity = plan.exit_quantity.min(context.inventory.base_position);
        if quantity <= Decimal::ZERO {
            return None;
        }

        let size_tier = classify_exit_size_tier(context, quantity).as_str().to_string();
        let reason = format!(
            "scalp exit {} pnl_bps={} expected_realized={} adverse_penalty={} age_secs={} target_bps={} flow={} drift500={} bid_queue={} ask_absorption={} inv={} qty={} stage={:?}",
            plan.trigger.as_str(),
            plan.pnl_bps,
            plan.expected_realized_edge_bps,
            plan.adverse_selection_penalty_bps,
            plan.position_age_secs,
            plan.target_bps,
            combined_flow_alignment(context),
            context.features.microstructure.book.micro_mid_drift_500ms_bps,
            context.features.microstructure.book.bid_queue_quality,
            context.features.microstructure.book.ask_absorption_score,
            context.inventory.base_position,
            quantity,
            plan.exit_stage,
        );

        Some(TradeIntent {
            intent_id: format!("scalp-exit-{}", new_id()),
            symbol: context.symbol,
            strategy: StrategyKind::Scalping,
            side: Side::Sell,
            quantity,
            limit_price: Some(plan.limit_price),
            max_slippage_bps: self.config.slippage_buffer_bps,
            post_only: plan.post_only,
            reduce_only: true,
            time_in_force: Some(plan.time_in_force),
            role: plan.role,
            exit_stage: Some(plan.exit_stage),
            exit_reason: Some(plan.trigger.as_str().to_string()),
            expected_edge_bps: plan.pnl_bps.abs().max(plan.target_bps / dec("2.0")),
            expected_fee_bps: estimated_entry_cost_bps(&self.config, plan.post_only),
            expected_slippage_bps: if plan.post_only {
                self.config.slippage_buffer_bps * dec("0.20")
            } else {
                self.config.slippage_buffer_bps
            },
            edge_after_cost_bps: plan.pnl_bps - estimated_entry_cost_bps(&self.config, plan.post_only),
            expected_realized_edge_bps: plan.expected_realized_edge_bps,
            adverse_selection_penalty_bps: plan.adverse_selection_penalty_bps,
            setup_type: Some(plan.trigger.as_str().to_string()),
            size_tier: Some(size_tier),
            reason,
            created_at: now_utc(),
            expires_at,
        })
    }
}

fn build_entry_candidates(
    context: &StrategyContext,
    best: &BestBidAsk,
    config: &ScalpingConfig,
    diagnostics: EntryDiagnostics,
    threshold: Decimal,
) -> Vec<EntryCandidate> {
    let mut candidates = Vec::new();
    let recent_markout_bad = recent_markout_bad(context);
    let severe_adverse_flow =
        diagnostics.momentum_signal_bps < dec("-0.5") || diagnostics.flow_alignment < dec("-0.5");
    let liquidity_sideways = context.features.top_of_book_depth_quote < dec("300")
        || diagnostics.book_instability >= dec("0.92");

    let passive_edge_after_cost =
        diagnostics.expected_edge_bps - estimated_entry_cost_bps(config, true);
    let passive_expected_realized = passive_edge_after_cost
        - diagnostics.adverse_selection_penalty_bps
        - diagnostics.flow_penalty_bps
        - diagnostics.book_instability_penalty_bps
        + diagnostics.absorption_bonus_bps
        + diagnostics.favorable_markout_bonus_bps;
    let passive_setup_quality = clamp_unit(
        diagnostics.bid_support * dec("0.35")
            + diagnostics.ask_fragility * dec("0.30")
            + positive_flow_score(diagnostics.flow_alignment) * dec("0.20")
            + positive_momentum_score(diagnostics.momentum_signal_bps) * dec("0.15"),
    );
    if !recent_markout_bad
        && !severe_adverse_flow
        && !liquidity_sideways
        && diagnostics.flow_alignment >= dec("0.12")
        && diagnostics.momentum_signal_bps >= Decimal::ZERO
        && diagnostics.bid_support >= dec("0.45")
        && diagnostics.ask_fragility >= dec("0.30")
        && passive_expected_realized > threshold
    {
        candidates.push(EntryCandidate {
            setup: EntrySetup::PassiveLongFavorable,
            expected_edge_bps: diagnostics.expected_edge_bps,
            edge_after_cost_bps: passive_edge_after_cost,
            expected_realized_edge_bps: passive_expected_realized,
            adverse_selection_penalty_bps: diagnostics.adverse_selection_penalty_bps,
            flow_penalty_bps: diagnostics.flow_penalty_bps,
            book_instability_penalty_bps: diagnostics.book_instability_penalty_bps,
            absorption_bonus_bps: diagnostics.absorption_bonus_bps,
            favorable_markout_bonus_bps: diagnostics.favorable_markout_bonus_bps,
            signal_quality: passive_setup_quality,
            post_only: true,
            time_in_force: TimeInForce::Gtc,
            limit_price: best.bid_price,
            probe_mode: false,
            strong_signal: passive_expected_realized >= threshold + dec("0.65")
                && strong_fill_quality(context)
                && diagnostics.book_instability <= dec("0.45"),
        });
    }

    let reversal_edge_after_cost =
        diagnostics.expected_edge_bps - estimated_entry_cost_bps(config, false);
    let reversal_expected_realized = reversal_edge_after_cost
        - diagnostics.adverse_selection_penalty_bps * dec("0.70")
        - diagnostics.flow_penalty_bps * dec("0.75")
        - diagnostics.book_instability_penalty_bps * dec("0.60")
        + diagnostics.absorption_bonus_bps * dec("1.20")
        + diagnostics.favorable_markout_bonus_bps;
    let reversal_quality = clamp_unit(
        positive_drift_score(context.features.microstructure.book.micro_mid_drift_500ms_bps)
            * dec("0.25")
            + normalized_score(
                context.features.microstructure.book.bid_absorption_score,
                dec("0.25"),
                dec("0.85"),
            ) * dec("0.35")
            + positive_flow_score(context.features.microstructure.flow.trade_flow_imbalance_500ms)
                * dec("0.20")
            + fill_quality_support(context) * dec("0.20"),
    );
    if !recent_markout_bad
        && diagnostics.book_instability < dec("0.88")
        && context.features.microstructure.book.bid_absorption_score >= dec("0.45")
        && context.features.microstructure.book.ask_absorption_score <= dec("0.65")
        && context.features.microstructure.book.micro_mid_drift_500ms_bps >= dec("-0.10")
        && context.features.microstructure.book.micro_mid_drift_1s_bps >= Decimal::ZERO
        && context.features.microstructure.flow.trade_flow_imbalance_1s >= dec("-0.10")
        && reversal_expected_realized > threshold
    {
        candidates.push(EntryCandidate {
            setup: EntrySetup::AbsorptionReversalLong,
            expected_edge_bps: diagnostics.expected_edge_bps + diagnostics.absorption_bonus_bps,
            edge_after_cost_bps: reversal_edge_after_cost,
            expected_realized_edge_bps: reversal_expected_realized,
            adverse_selection_penalty_bps: diagnostics.adverse_selection_penalty_bps * dec("0.70"),
            flow_penalty_bps: diagnostics.flow_penalty_bps * dec("0.75"),
            book_instability_penalty_bps: diagnostics.book_instability_penalty_bps * dec("0.60"),
            absorption_bonus_bps: diagnostics.absorption_bonus_bps * dec("1.20"),
            favorable_markout_bonus_bps: diagnostics.favorable_markout_bonus_bps,
            signal_quality: reversal_quality,
            post_only: false,
            time_in_force: TimeInForce::Ioc,
            limit_price: best.ask_price,
            probe_mode: false,
            strong_signal: reversal_expected_realized >= threshold + dec("0.85")
                && strong_fill_quality(context)
                && diagnostics.bid_support >= dec("0.55"),
        });
    }

    let probe_edge_after_cost = passive_edge_after_cost;
    let probe_expected_realized = probe_edge_after_cost
        - diagnostics.adverse_selection_penalty_bps
        - diagnostics.flow_penalty_bps
        - diagnostics.book_instability_penalty_bps
        + diagnostics.absorption_bonus_bps * dec("0.60")
        + diagnostics.favorable_markout_bonus_bps * dec("0.60");
    let probe_threshold = probe_threshold(context.symbol);
    if !recent_markout_bad
        && !severe_adverse_flow
        && !liquidity_sideways
        && diagnostics.book_instability < dec("0.65")
        && diagnostics.momentum_signal_bps >= dec("-0.50")
        && diagnostics.flow_alignment >= dec("-0.50")
        && probe_edge_after_cost > probe_threshold
        && probe_expected_realized > threshold
        && passive_setup_quality >= dec("0.35")
    {
        candidates.push(EntryCandidate {
            setup: EntrySetup::ProbeEntry,
            expected_edge_bps: diagnostics.expected_edge_bps,
            edge_after_cost_bps: probe_edge_after_cost,
            expected_realized_edge_bps: probe_expected_realized,
            adverse_selection_penalty_bps: diagnostics.adverse_selection_penalty_bps,
            flow_penalty_bps: diagnostics.flow_penalty_bps,
            book_instability_penalty_bps: diagnostics.book_instability_penalty_bps,
            absorption_bonus_bps: diagnostics.absorption_bonus_bps * dec("0.60"),
            favorable_markout_bonus_bps: diagnostics.favorable_markout_bonus_bps * dec("0.60"),
            signal_quality: passive_setup_quality.max(dec("0.35")),
            post_only: true,
            time_in_force: TimeInForce::Gtc,
            limit_price: best.bid_price,
            probe_mode: true,
            strong_signal: false,
        });
    }

    candidates
}

fn entry_diagnostics(context: &StrategyContext, config: &ScalpingConfig) -> EntryDiagnostics {
    let flow_alignment = combined_flow_alignment(context);
    let momentum_signal_bps = combined_momentum_signal_bps(context);
    let bid_support = bid_support_score(context);
    let ask_fragility = ask_fragility_score(context);
    let book_support = clamp_unit((bid_support + ask_fragility) / dec("2.0"));
    let toxicity_pressure = normalized_score(
        context.features.toxicity_score,
        dec("0.20"),
        config.max_toxicity_score.max(dec("0.85")),
    );
    let book_instability = context.features.microstructure.book.book_instability_score;
    let favorable_markout_bonus_bps = favorable_markout_bonus_bps(context);
    let absorption_bonus_bps = clamp_unit(
        normalized_score(
            context.features.microstructure.book.bid_absorption_score,
            dec("0.15"),
            dec("0.85"),
        ) * dec("0.60")
            + ask_fragility * dec("0.40"),
    ) * dec("0.55");
    let expected_edge_bps = (positive_momentum_score(momentum_signal_bps) * dec("1.50")
        + positive_flow_score(flow_alignment) * dec("1.35")
        + book_support * dec("0.90")
        + positive_drift_score(context.features.microstructure.book.micro_mid_drift_500ms_bps)
            * dec("0.45")
        + regime_edge_bonus(context.regime.state)
        + favorable_markout_bonus_bps)
        .max(Decimal::ZERO);
    let adverse_selection_penalty_bps = adverse_selection_penalty_bps(context, momentum_signal_bps);
    let flow_penalty_bps = flow_penalty_bps(context, flow_alignment, bid_support);
    let book_instability_penalty_bps = book_instability_penalty_bps(context, toxicity_pressure);
    let best_expected_realized_edge_bps = expected_edge_bps
        - estimated_entry_cost_bps(config, true)
        - adverse_selection_penalty_bps
        - flow_penalty_bps
        - book_instability_penalty_bps
        + absorption_bonus_bps
        + favorable_markout_bonus_bps;

    EntryDiagnostics {
        expected_edge_bps,
        best_expected_realized_edge_bps,
        adverse_selection_penalty_bps,
        flow_penalty_bps,
        book_instability_penalty_bps,
        absorption_bonus_bps,
        favorable_markout_bonus_bps,
        flow_alignment,
        momentum_signal_bps,
        ask_fragility,
        bid_support,
        book_instability,
    }
}

fn analyze_exit(
    context: &StrategyContext,
    config: &ScalpingConfig,
    best: &BestBidAsk,
    average_entry: Decimal,
    base_quote_size: Decimal,
    max_position: Decimal,
) -> Option<ExitPlan> {
    let exit_price = best.bid_price;
    if average_entry <= Decimal::ZERO || exit_price <= Decimal::ZERO {
        return None;
    }

    let pnl_bps = ((exit_price - average_entry) / average_entry) * Decimal::from(10_000u32);
    let position_age_secs = context
        .inventory
        .position_opened_at
        .map(|opened_at| (context.features.computed_at - opened_at).whole_seconds())
        .unwrap_or_default();
    let flow_alignment = combined_flow_alignment(context);
    let adverse_markout_score = adverse_markout_score(context);
    let flow_reversal_score = flow_reversal_score(context);
    let liquidity_collapse_score = liquidity_collapse_score(context);
    let absorption_failure_score = absorption_failure_score(context);
    let inventory_pressure = inventory_pressure(context, max_position);
    let stale_inventory = position_age_secs >= config.stale_hold_secs
        || (position_age_secs >= config.soft_hold_secs && inventory_pressure >= dec("0.60"));
    let target_bps = micro_target_bps(context);
    let target_decay = position_age_secs >= config.soft_hold_secs
        && pnl_bps >= dec("0.25")
        && (flow_alignment <= dec("0.08")
            || context.features.microstructure.book.micro_mid_drift_500ms_bps <= dec("0.10")
            || context.features.microstructure.book.ask_absorption_score >= dec("0.45"));
    let micro_target_hit = pnl_bps >= target_bps && target_decay;
    let aggressive_unwind = position_age_secs >= config.aggressive_exit_secs
        || adverse_markout_score >= dec("0.78")
        || liquidity_collapse_score >= dec("0.78")
        || flow_reversal_score >= dec("0.82")
        || (stale_inventory
            && (flow_reversal_score >= dec("0.55")
                || adverse_markout_score >= dec("0.50")));

    let trigger = if micro_target_hit {
        ExitTrigger::MicroTargetHit
    } else if adverse_markout_score >= dec("0.62") {
        ExitTrigger::AdverseMarkout
    } else if liquidity_collapse_score >= dec("0.68") {
        ExitTrigger::LiquidityCollapse
    } else if absorption_failure_score >= dec("0.66") {
        ExitTrigger::AbsorptionFailure
    } else if flow_reversal_score >= dec("0.60") {
        ExitTrigger::FlowReversal
    } else if stale_inventory {
        ExitTrigger::StaleInventory
    } else {
        return None;
    };

    let (role, exit_stage, post_only, time_in_force, limit_price) = if aggressive_unwind {
        (
            if position_age_secs >= config.aggressive_exit_secs {
                IntentRole::EmergencyExit
            } else {
                IntentRole::ForcedUnwind
            },
            if position_age_secs >= config.aggressive_exit_secs {
                ExitStage::Emergency
            } else {
                ExitStage::Aggressive
            },
            false,
            TimeInForce::Ioc,
            best.bid_price,
        )
    } else if matches!(trigger, ExitTrigger::MicroTargetHit) {
        (
            IntentRole::PassiveProfitTake,
            ExitStage::Passive,
            true,
            TimeInForce::Gtc,
            best.ask_price,
        )
    } else {
        (
            IntentRole::DefensiveExit,
            ExitStage::Tighten,
            true,
            TimeInForce::Gtc,
            tightened_exit_price(best),
        )
    };

    let partial_exit_size = (base_quote_size * dec("1.25"))
        .max(context.inventory.base_position * dec("0.40"))
        .min(max_position.max(base_quote_size));
    let exit_quantity = if aggressive_unwind || inventory_pressure >= dec("0.72") {
        context.inventory.base_position
    } else {
        partial_exit_size.min(context.inventory.base_position)
    };
    let adverse_selection_penalty_bps = adverse_markout_score * dec("0.40")
        + flow_reversal_score * dec("0.35")
        + liquidity_collapse_score * dec("0.30")
        + absorption_failure_score * dec("0.25");
    let expected_realized_edge_bps =
        pnl_bps - estimated_entry_cost_bps(config, post_only) - adverse_selection_penalty_bps;

    Some(ExitPlan {
        trigger,
        role,
        exit_stage,
        post_only,
        time_in_force,
        limit_price,
        exit_quantity,
        pnl_bps,
        expected_realized_edge_bps,
        adverse_selection_penalty_bps,
        position_age_secs,
        target_bps,
    })
}

fn entry_size(
    context: &StrategyContext,
    best: &BestBidAsk,
    base_quote_size: Decimal,
    max_position: Decimal,
    candidate: EntryCandidate,
    threshold: Decimal,
) -> (Decimal, SizeTier) {
    let available = (max_position - context.inventory.base_position).max(Decimal::ZERO);
    if available <= Decimal::ZERO {
        return (Decimal::ZERO, SizeTier::Zero);
    }

    let inventory_factor = (Decimal::ONE - inventory_pressure(context, max_position) * dec("0.75"))
        .max(dec("0.20"));
    let toxicity_factor = (Decimal::ONE
        - normalized_score(context.features.toxicity_score, dec("0.25"), dec("0.90")) * dec("0.45"))
    .max(dec("0.35"));
    let fill_quality_factor = fill_quality_sizing_factor(context);
    let base_multiplier = if candidate.probe_mode {
        dec("0.18") + candidate.signal_quality * dec("0.12")
    } else if candidate.strong_signal {
        dec("1.10") + candidate.signal_quality * dec("0.45")
    } else {
        dec("0.65") + candidate.signal_quality * dec("0.35")
    };
    let edge_factor = if candidate.expected_realized_edge_bps <= threshold {
        dec("0.75")
    } else {
        (dec("0.95")
            + clamp_unit((candidate.expected_realized_edge_bps - threshold) / dec("1.20"))
                * dec("0.45"))
        .min(dec("1.40"))
    };

    let mut quantity = base_quote_size
        * base_multiplier
        * inventory_factor
        * toxicity_factor
        * fill_quality_factor
        * edge_factor;
    let min_deployable_quantity = min_deployable_quantity(
        best.ask_price.max(candidate.limit_price),
        context.local_min_notional_quote,
    );
    if quantity < min_deployable_quantity && available >= min_deployable_quantity {
        quantity = min_deployable_quantity.max(quantity);
    }
    quantity = quantity.min(available);

    let size_tier = if quantity <= Decimal::ZERO {
        SizeTier::Zero
    } else if candidate.probe_mode {
        SizeTier::Probe
    } else if candidate.strong_signal {
        SizeTier::Strong
    } else {
        SizeTier::Normal
    };

    (quantity, size_tier)
}

fn classify_exit_size_tier(context: &StrategyContext, quantity: Decimal) -> SizeTier {
    if quantity <= Decimal::ZERO || context.soft_inventory_base <= Decimal::ZERO {
        return SizeTier::Zero;
    }
    let ratio = quantity / context.soft_inventory_base;
    if ratio <= dec("0.30") {
        SizeTier::Probe
    } else if ratio <= dec("0.85") {
        SizeTier::Normal
    } else {
        SizeTier::Strong
    }
}

fn entry_block_reason(
    context: &StrategyContext,
    diagnostics: EntryDiagnostics,
    threshold: Decimal,
) -> &'static str {
    if recent_markout_bad(context) {
        "markout_quality"
    } else if diagnostics.flow_alignment <= dec("-0.35") {
        "adverse_flow"
    } else if diagnostics.momentum_signal_bps <= dec("-0.75") {
        "adverse_momentum"
    } else if diagnostics.best_expected_realized_edge_bps <= Decimal::ZERO {
        "expected_edge_non_positive"
    } else if diagnostics.best_expected_realized_edge_bps <= threshold {
        "expected_edge_below_threshold"
    } else if diagnostics.book_instability >= dec("0.70") {
        "book_instability"
    } else if diagnostics.bid_support <= dec("0.25") {
        "weak_bid_support"
    } else {
        "setup_invalid"
    }
}

fn entry_block_message(
    context: &StrategyContext,
    diagnostics: EntryDiagnostics,
    threshold: Decimal,
) -> &'static str {
    match entry_block_reason(context, diagnostics, threshold) {
        "markout_quality" => "scalping blocked by poor recent fill markout quality",
        "expected_edge_non_positive" => "scalping blocked by non-positive expected realized edge",
        "expected_edge_below_threshold" => "scalping blocked by expected realized edge below threshold",
        "adverse_flow" => "scalping blocked by adverse flow alignment",
        "adverse_momentum" => "scalping blocked by adverse short momentum",
        "book_instability" => "scalping blocked by unstable book dynamics",
        "weak_bid_support" => "scalping blocked by weak bid support",
        _ => "scalping blocked by invalid microstructure setup",
    }
}

fn combined_flow_alignment(context: &StrategyContext) -> Decimal {
    let flow = &context.features.microstructure.flow;
    (flow.trade_flow_imbalance_250ms * dec("0.45"))
        + (flow.trade_flow_imbalance_500ms * dec("0.35"))
        + (flow.trade_flow_imbalance_1s * dec("0.20"))
}

fn combined_momentum_signal_bps(context: &StrategyContext) -> Decimal {
    let drift = context.features.microstructure.book.micro_mid_drift_500ms_bps;
    (context.features.local_momentum_bps * dec("0.40"))
        + (context.features.momentum_1s_bps * dec("0.30"))
        + (context.features.momentum_5s_bps * dec("0.15"))
        + (drift * dec("0.15"))
}

fn bid_support_score(context: &StrategyContext) -> Decimal {
    let book = &context.features.microstructure.book;
    clamp_unit(
        normalized_score(book.orderbook_imbalance_l3, dec("0.02"), dec("0.30")) * dec("0.25")
            + normalized_score(book.orderbook_imbalance_l5, dec("0.02"), dec("0.25")) * dec("0.15")
            + book.bid_queue_quality * dec("0.20")
            + book.bid_absorption_score * dec("0.20")
            + normalized_score(
                Decimal::from(book.best_bid_persistence_ms.max(0) as u64),
                dec("90"),
                dec("1400"),
            ) * dec("0.10")
            + positive_flow_score(context.features.microstructure.flow.trade_flow_imbalance_500ms)
                * dec("0.10"),
    )
}

fn ask_fragility_score(context: &StrategyContext) -> Decimal {
    let book = &context.features.microstructure.book;
    clamp_unit(
        normalized_score(
            (book.ask_cancel_rate - book.ask_add_rate).max(Decimal::ZERO),
            dec("0"),
            dec("2500"),
        ) * dec("0.25")
            + (Decimal::ONE - book.ask_queue_quality) * dec("0.20")
            + (Decimal::ONE
                - normalized_score(
                    Decimal::from(book.best_ask_persistence_ms.max(0) as u64),
                    dec("80"),
                    dec("1200"),
                )) * dec("0.15")
            + positive_flow_score(context.features.microstructure.flow.trade_flow_imbalance_250ms)
                * dec("0.20")
            + positive_drift_score(book.micro_mid_drift_500ms_bps) * dec("0.20"),
    )
}

fn adverse_selection_penalty_bps(context: &StrategyContext, momentum_signal_bps: Decimal) -> Decimal {
    let book = &context.features.microstructure.book;
    let flow = &context.features.microstructure.flow;
    let adverse_momentum =
        normalized_score((-momentum_signal_bps).max(Decimal::ZERO), dec("0.40"), dec("4.0"));
    let adverse_flow_250 = normalized_score(
        (-flow.trade_flow_imbalance_250ms).max(Decimal::ZERO),
        dec("0.10"),
        dec("0.70"),
    );
    let adverse_flow_500 = normalized_score(
        (-flow.trade_flow_imbalance_500ms).max(Decimal::ZERO),
        dec("0.08"),
        dec("0.60"),
    );
    let adverse_book =
        normalized_score((-book.orderbook_imbalance_l3).max(Decimal::ZERO), dec("0.03"), dec("0.30"));
    let ask_absorption = normalized_score(book.ask_absorption_score, dec("0.20"), dec("0.90"));

    adverse_momentum * dec("0.25")
        + adverse_flow_250 * dec("0.35")
        + adverse_flow_500 * dec("0.25")
        + adverse_book * dec("0.20")
        + ask_absorption * dec("0.20")
}

fn flow_penalty_bps(context: &StrategyContext, flow_alignment: Decimal, bid_support: Decimal) -> Decimal {
    let negative_flow = normalized_score((-flow_alignment).max(Decimal::ZERO), dec("0.05"), dec("0.60"));
    let weak_bid = Decimal::ONE - bid_support;
    let flow = &context.features.microstructure.flow;
    negative_flow * dec("0.40")
        + weak_bid.max(Decimal::ZERO) * dec("0.25")
        + normalized_score(
            (flow.market_sell_notional_500ms - flow.market_buy_notional_500ms).max(Decimal::ZERO),
            dec("50"),
            dec("1000"),
        ) * dec("0.15")
}

fn book_instability_penalty_bps(context: &StrategyContext, toxicity_pressure: Decimal) -> Decimal {
    let book = &context.features.microstructure.book;
    book.book_instability_score * dec("0.40")
        + book.ask_spoofing_score * dec("0.18")
        + toxicity_pressure * dec("0.22")
        + normalized_score(
            (book.ask_cancel_rate - book.ask_add_rate).abs(),
            dec("800"),
            dec("4000"),
        ) * dec("0.12")
}

fn favorable_markout_bonus_bps(context: &StrategyContext) -> Decimal {
    if context.fill_quality.samples < 4 {
        return dec("0.05");
    }

    let favorable_markout = normalized_score(
        context.fill_quality.avg_markout_1s_bps.max(Decimal::ZERO),
        dec("0.05"),
        dec("0.80"),
    );
    favorable_markout * dec("0.22")
        + context.fill_quality.positive_markout_rate * dec("0.18")
        + normalized_score(
            context.fill_quality.fill_quality_score.max(Decimal::ZERO),
            dec("0.05"),
            dec("1.00"),
        ) * dec("0.12")
}

fn fill_quality_support(context: &StrategyContext) -> Decimal {
    if context.fill_quality.samples < 4 {
        dec("0.45")
    } else {
        clamp_unit(
            context.fill_quality.positive_markout_rate * dec("0.50")
                + (Decimal::ONE - context.fill_quality.adverse_selection_rate) * dec("0.30")
                + normalized_score(
                    context.fill_quality.fill_quality_score.max(Decimal::ZERO),
                    dec("0.05"),
                    dec("1.00"),
                ) * dec("0.20"),
        )
    }
}

fn recent_markout_bad(context: &StrategyContext) -> bool {
    context.fill_quality.samples >= 6
        && (context.fill_quality.avg_markout_500ms_bps <= dec("-0.20")
            || context.fill_quality.avg_markout_1s_bps <= dec("-0.15")
            || context.fill_quality.adverse_selection_rate >= dec("0.58")
            || context.fill_quality.fill_quality_score <= dec("-0.12"))
}

fn strong_fill_quality(context: &StrategyContext) -> bool {
    context.fill_quality.samples < 5
        || (context.fill_quality.positive_markout_rate >= dec("0.55")
            && context.fill_quality.adverse_selection_rate <= dec("0.35")
            && context.fill_quality.fill_quality_score >= dec("0.10"))
}

fn adverse_hits(context: &StrategyContext) -> u64 {
    u64::from(context.fill_quality.adverse_selection_rate >= dec("0.45"))
}

fn positive_momentum_score(value: Decimal) -> Decimal {
    normalized_score(value.max(Decimal::ZERO), dec("0.50"), dec("8.0"))
}

fn positive_flow_score(value: Decimal) -> Decimal {
    normalized_score(value.max(Decimal::ZERO), dec("0.04"), dec("0.45"))
}

fn positive_drift_score(value: Decimal) -> Decimal {
    normalized_score(value.max(Decimal::ZERO), dec("0.05"), dec("1.50"))
}

fn regime_edge_bonus(state: RegimeState) -> Decimal {
    match state {
        RegimeState::TrendUp => dec("0.30"),
        RegimeState::Range => dec("0.12"),
        _ => Decimal::ZERO,
    }
}

fn micro_target_bps(context: &StrategyContext) -> Decimal {
    let volatility_floor = match context.features.volatility_regime {
        VolatilityRegime::Low => dec("1.1"),
        VolatilityRegime::Medium => dec("1.4"),
        VolatilityRegime::High => dec("1.8"),
    };
    (context.features.realized_volatility_bps * dec("0.35"))
        .max(volatility_floor)
        .min(dec("4.5"))
}

fn flow_reversal_score(context: &StrategyContext) -> Decimal {
    let flow = &context.features.microstructure.flow;
    let book = &context.features.microstructure.book;
    clamp_unit(
        normalized_score(
            (-flow.trade_flow_imbalance_250ms).max(Decimal::ZERO),
            dec("0.10"),
            dec("0.70"),
        ) * dec("0.35")
            + normalized_score(
                (-flow.trade_flow_imbalance_500ms).max(Decimal::ZERO),
                dec("0.08"),
                dec("0.60"),
            ) * dec("0.20")
            + normalized_score(
                (-context.features.momentum_1s_bps).max(Decimal::ZERO),
                dec("0.50"),
                dec("5.0"),
            ) * dec("0.20")
            + normalized_score(
                (-book.micro_mid_drift_500ms_bps).max(Decimal::ZERO),
                dec("0.10"),
                dec("2.0"),
            ) * dec("0.15")
            + normalized_score(book.ask_absorption_score, dec("0.25"), dec("0.90")) * dec("0.10"),
    )
}

fn adverse_markout_score(context: &StrategyContext) -> Decimal {
    if context.fill_quality.samples == 0 {
        return Decimal::ZERO;
    }

    clamp_unit(
        normalized_score(
            (-context.fill_quality.avg_markout_500ms_bps).max(Decimal::ZERO),
            dec("0.05"),
            dec("0.80"),
        ) * dec("0.40")
            + normalized_score(
                (-context.fill_quality.avg_markout_1s_bps).max(Decimal::ZERO),
                dec("0.05"),
                dec("0.80"),
            ) * dec("0.25")
            + normalized_score(
                context.fill_quality.adverse_selection_rate,
                dec("0.15"),
                dec("0.85"),
            ) * dec("0.20")
            + normalized_score(
                (-context.fill_quality.fill_quality_score).max(Decimal::ZERO),
                dec("0.05"),
                dec("1.00"),
            ) * dec("0.15"),
    )
}

fn liquidity_collapse_score(context: &StrategyContext) -> Decimal {
    let book = &context.features.microstructure.book;
    clamp_unit(
        normalized_score(
            (book.bid_cancel_rate - book.bid_add_rate).max(Decimal::ZERO),
            dec("0"),
            dec("2500"),
        ) * dec("0.35")
            + (Decimal::ONE - book.bid_queue_quality) * dec("0.25")
            + normalized_score(book.book_instability_score, dec("0.25"), dec("0.90")) * dec("0.20")
            + normalized_score(
                context.features.spread_zscore.max(Decimal::ZERO),
                dec("0.20"),
                dec("2.00"),
            ) * dec("0.10")
            + (Decimal::ONE
                - normalized_score(
                    Decimal::from(book.best_bid_persistence_ms.max(0) as u64),
                    dec("80"),
                    dec("1200"),
                )) * dec("0.10"),
    )
}

fn absorption_failure_score(context: &StrategyContext) -> Decimal {
    let book = &context.features.microstructure.book;
    clamp_unit(
        normalized_score(
            (book.ask_absorption_score - book.bid_absorption_score).max(Decimal::ZERO),
            dec("0.05"),
            dec("0.60"),
        ) * dec("0.45")
            + normalized_score(
                (-book.micro_mid_drift_500ms_bps).max(Decimal::ZERO),
                dec("0.10"),
                dec("2.00"),
            ) * dec("0.20")
            + (Decimal::ONE - book.bid_queue_quality) * dec("0.20")
            + normalized_score(
                (-context.features.microstructure.flow.trade_flow_imbalance_250ms).max(Decimal::ZERO),
                dec("0.08"),
                dec("0.60"),
            ) * dec("0.15"),
    )
}

fn tightened_exit_price(best: &BestBidAsk) -> Decimal {
    let spread = (best.ask_price - best.bid_price).max(Decimal::ZERO);
    let tightened = best.ask_price - (spread * dec("0.35"));
    if tightened <= best.bid_price {
        best.ask_price
    } else {
        tightened
    }
}

fn inventory_pressure(context: &StrategyContext, max_position: Decimal) -> Decimal {
    if max_position <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        clamp_unit((context.inventory.base_position / max_position).max(Decimal::ZERO))
    }
}

fn estimated_entry_cost_bps(config: &ScalpingConfig, post_only: bool) -> Decimal {
    if post_only {
        (config.taker_fee_bps * dec("0.35")) + (config.slippage_buffer_bps * dec("0.15"))
    } else {
        config.taker_fee_bps + config.slippage_buffer_bps
    }
}

fn min_deployable_quantity(limit_price: Decimal, local_min_notional_quote: Decimal) -> Decimal {
    if limit_price <= Decimal::ZERO || local_min_notional_quote <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        local_min_notional_quote / limit_price
    }
}

fn has_deployable_notional(
    quantity: Decimal,
    limit_price: Decimal,
    local_min_notional_quote: Decimal,
) -> bool {
    quantity > Decimal::ZERO
        && limit_price > Decimal::ZERO
        && (quantity * limit_price) >= local_min_notional_quote
}

fn fill_quality_sizing_factor(context: &StrategyContext) -> Decimal {
    if context.fill_quality.samples < 4 {
        return dec("1.00");
    }

    let positive_score = normalized_score(
        context.fill_quality.fill_quality_score.max(Decimal::ZERO),
        dec("0.05"),
        dec("1.00"),
    );
    let adverse_drag = normalized_score(
        context.fill_quality.adverse_selection_rate,
        dec("0.20"),
        dec("0.80"),
    );
    (dec("0.80") + positive_score * dec("0.25") - adverse_drag * dec("0.20"))
        .max(dec("0.55"))
        .min(dec("1.20"))
}

fn expected_realized_threshold(symbol: domain::Symbol) -> Decimal {
    match symbol {
        domain::Symbol::BtcUsdc => dec("0.35"),
        domain::Symbol::EthUsdc => dec("0.45"),
    }
}

fn probe_threshold(symbol: domain::Symbol) -> Decimal {
    match symbol {
        domain::Symbol::BtcUsdc => dec("0.35"),
        domain::Symbol::EthUsdc => dec("0.45"),
    }
}

fn available_order_slots(context: &StrategyContext) -> u32 {
    context
        .max_open_orders_for_symbol
        .saturating_sub(context.open_bot_orders_for_symbol)
}

fn normalized_score(value: Decimal, low: Decimal, high: Decimal) -> Decimal {
    if high <= low {
        return Decimal::ZERO;
    }
    if value <= low {
        Decimal::ZERO
    } else if value >= high {
        Decimal::ONE
    } else {
        clamp_unit((value - low) / (high - low))
    }
}

fn clamp_unit(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        Decimal::ZERO
    } else if value >= Decimal::ONE {
        Decimal::ONE
    } else {
        value
    }
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

fn standby(reason: &str) -> StrategyOutcome {
    StrategyOutcome {
        intents: Vec::new(),
        standby_reason: Some(reason.to_string()),
        entry_block_reason: None,
        best_expected_realized_edge_bps: None,
        adverse_selection_hits: 0,
    }
}

fn blocked(
    reason: &str,
    entry_block_reason: &str,
    best_expected_realized_edge_bps: Option<Decimal>,
    adverse_selection_hits: u64,
) -> StrategyOutcome {
    StrategyOutcome {
        intents: Vec::new(),
        standby_reason: Some(reason.to_string()),
        entry_block_reason: Some(entry_block_reason.to_string()),
        best_expected_realized_edge_bps,
        adverse_selection_hits,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{
        BookDynamicsFeatures, FeatureSnapshot, FillQualitySnapshot, FlowFeatures, InventorySnapshot,
        MicrostructureSnapshot, RegimeDecision, RiskMode, RuntimeState, Symbol,
    };

    fn sample_context() -> StrategyContext {
        StrategyContext {
            symbol: Symbol::BtcUsdc,
            best_bid_ask: Some(BestBidAsk {
                symbol: Symbol::BtcUsdc,
                bid_price: dec("66690.00"),
                bid_quantity: dec("1.8"),
                ask_price: dec("66692.00"),
                ask_quantity: dec("1.6"),
                observed_at: now_utc(),
            }),
            features: FeatureSnapshot {
                symbol: Symbol::BtcUsdc,
                microprice: Some(dec("66691.00")),
                top_of_book_depth_quote: dec("9000"),
                orderbook_imbalance: Some(dec("0.22")),
                orderbook_imbalance_rolling: dec("0.18"),
                realized_volatility_bps: dec("5.0"),
                vwap: dec("99.98"),
                vwap_distance_bps: dec("2.2"),
                spread_bps: dec("2.0"),
                spread_mean_bps: dec("1.6"),
                spread_std_bps: dec("0.2"),
                spread_zscore: dec("0.30"),
                trade_flow_imbalance: dec("0.28"),
                trade_flow_rate: dec("1.5"),
                trade_rate_per_sec: dec("2.0"),
                tick_rate_per_sec: dec("3.0"),
                tape_speed: dec("2.5"),
                momentum_1s_bps: dec("4.0"),
                momentum_5s_bps: dec("2.5"),
                momentum_15s_bps: dec("1.0"),
                local_momentum_bps: dec("5.0"),
                liquidity_score: dec("8200"),
                toxicity_score: dec("0.20"),
                microstructure: MicrostructureSnapshot {
                    flow: FlowFeatures {
                        trade_flow_imbalance_250ms: dec("0.22"),
                        trade_flow_imbalance_500ms: dec("0.24"),
                        trade_flow_imbalance_1s: dec("0.18"),
                        market_buy_notional_500ms: dec("600"),
                        market_sell_notional_500ms: dec("280"),
                        market_buy_notional_1s: dec("1100"),
                        market_sell_notional_1s: dec("640"),
                    },
                    book: BookDynamicsFeatures {
                        orderbook_imbalance_l3: dec("0.22"),
                        orderbook_imbalance_l5: dec("0.18"),
                        bid_add_rate: dec("1800"),
                        ask_add_rate: dec("900"),
                        bid_cancel_rate: dec("700"),
                        ask_cancel_rate: dec("1550"),
                        best_bid_persistence_ms: 820,
                        best_ask_persistence_ms: 210,
                        micro_mid_drift_500ms_bps: dec("0.45"),
                        micro_mid_drift_1s_bps: dec("0.30"),
                        bid_absorption_score: dec("0.54"),
                        ask_absorption_score: dec("0.22"),
                        bid_spoofing_score: dec("0.15"),
                        ask_spoofing_score: dec("0.22"),
                        bid_queue_quality: dec("0.62"),
                        ask_queue_quality: dec("0.30"),
                        book_instability_score: dec("0.22"),
                    },
                },
                volatility_regime: VolatilityRegime::Medium,
                computed_at: now_utc(),
            },
            fill_quality: FillQualitySnapshot {
                avg_markout_500ms_bps: dec("0.18"),
                avg_markout_1s_bps: dec("0.24"),
                avg_markout_3s_bps: dec("0.10"),
                avg_markout_5s_bps: dec("0.05"),
                positive_markout_rate: dec("0.62"),
                adverse_selection_rate: dec("0.18"),
                fill_quality_score: dec("0.35"),
                samples: 12,
            },
            regime: RegimeDecision {
                symbol: Symbol::BtcUsdc,
                state: RegimeState::TrendUp,
                confidence: dec("0.80"),
                reason: "trend up".to_string(),
                decided_at: now_utc(),
            },
            inventory: InventorySnapshot {
                symbol: Symbol::BtcUsdc,
                base_position: Decimal::ZERO,
                quote_position: dec("1000"),
                mark_price: Some(dec("66691.00")),
                average_entry_price: None,
                position_opened_at: None,
                last_fill_at: None,
                first_reduce_at: None,
                updated_at: now_utc(),
            },
            soft_inventory_base: dec("0.020"),
            max_inventory_base: dec("0.050"),
            local_min_notional_quote: dec("10.00"),
            open_bot_orders_for_symbol: 0,
            max_open_orders_for_symbol: 4,
            runtime_state: RuntimeState::Trading,
            risk_mode: RiskMode::Normal,
        }
    }

    #[test]
    fn favorable_flow_authorizes_scalp_entry() {
        let outcome = ScalpingStrategy::default().evaluate(&sample_context());

        assert_eq!(outcome.intents.len(), 1);
        let intent = &outcome.intents[0];
        assert_eq!(intent.side, Side::Buy);
        assert!(intent.expected_realized_edge_bps > dec("0.35"));
        assert_eq!(intent.setup_type.as_deref(), Some("passive_long_favorable"));
    }

    #[test]
    fn adverse_flow_blocks_entry() {
        let mut context = sample_context();
        context.features.microstructure.flow.trade_flow_imbalance_250ms = dec("-0.65");
        context.features.microstructure.flow.trade_flow_imbalance_500ms = dec("-0.55");
        context.features.microstructure.flow.trade_flow_imbalance_1s = dec("-0.40");
        context.features.local_momentum_bps = dec("-2.0");
        context.features.momentum_1s_bps = dec("-1.4");

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(outcome.entry_block_reason.as_deref(), Some("adverse_flow"));
    }

    #[test]
    fn bad_recent_markout_blocks_probing() {
        let mut context = sample_context();
        context.features.microstructure.flow.trade_flow_imbalance_250ms = dec("0.10");
        context.features.microstructure.flow.trade_flow_imbalance_500ms = dec("0.12");
        context.features.microstructure.flow.trade_flow_imbalance_1s = dec("0.08");
        context.features.local_momentum_bps = dec("1.0");
        context.features.momentum_1s_bps = dec("0.8");
        context.fill_quality.avg_markout_500ms_bps = dec("-0.35");
        context.fill_quality.avg_markout_1s_bps = dec("-0.25");
        context.fill_quality.adverse_selection_rate = dec("0.72");
        context.fill_quality.fill_quality_score = dec("-0.30");

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(outcome.entry_block_reason.as_deref(), Some("markout_quality"));
    }

    #[test]
    fn absorption_favorable_gives_expected_realized_bonus() {
        let baseline = sample_context();
        let baseline_outcome = ScalpingStrategy::default().evaluate(&baseline);
        let baseline_edge = baseline_outcome.intents[0].expected_realized_edge_bps;

        let mut boosted = sample_context();
        boosted.features.microstructure.book.bid_absorption_score = dec("0.90");
        boosted.features.microstructure.book.ask_absorption_score = dec("0.08");
        boosted.features.microstructure.book.micro_mid_drift_500ms_bps = dec("0.80");

        let boosted_outcome = ScalpingStrategy::default().evaluate(&boosted);
        let boosted_edge = boosted_outcome.intents[0].expected_realized_edge_bps;

        assert!(boosted_edge > baseline_edge);
    }

    #[test]
    fn adverse_markout_triggers_defensive_exit() {
        let mut context = sample_context();
        context.inventory.base_position = dec("0.015");
        context.inventory.average_entry_price = Some(dec("66740.00"));
        context.inventory.position_opened_at =
            Some(context.features.computed_at - time::Duration::seconds(4));
        context.fill_quality.avg_markout_500ms_bps = dec("-0.40");
        context.fill_quality.avg_markout_1s_bps = dec("-0.72");
        context.fill_quality.adverse_selection_rate = dec("0.92");
        context.fill_quality.fill_quality_score = dec("-0.85");
        context.features.microstructure.flow.trade_flow_imbalance_250ms = dec("-0.28");
        context.features.microstructure.flow.trade_flow_imbalance_500ms = dec("-0.20");
        context.features.microstructure.book.ask_absorption_score = dec("0.78");
        context.features.microstructure.book.bid_queue_quality = dec("0.22");
        context.features.momentum_1s_bps = dec("-1.2");
        context.features.local_momentum_bps = dec("-0.8");

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert_eq!(outcome.intents.len(), 1);
        let intent = &outcome.intents[0];
        assert_eq!(intent.side, Side::Sell);
        assert!(intent.reduce_only);
        assert_eq!(intent.exit_reason.as_deref(), Some("adverse_markout"));
    }

    #[test]
    fn sizing_increases_only_on_strong_signal() {
        let strategy = ScalpingStrategy::default();
        let normal_outcome = strategy.evaluate(&sample_context());
        let normal_intent = &normal_outcome.intents[0];

        let mut strong = sample_context();
        strong.features.microstructure.flow.trade_flow_imbalance_250ms = dec("0.42");
        strong.features.microstructure.flow.trade_flow_imbalance_500ms = dec("0.36");
        strong.features.microstructure.flow.trade_flow_imbalance_1s = dec("0.28");
        strong.features.local_momentum_bps = dec("7.5");
        strong.features.momentum_1s_bps = dec("6.0");
        strong.features.microstructure.book.bid_absorption_score = dec("0.88");
        strong.features.microstructure.book.bid_queue_quality = dec("0.82");
        strong.features.microstructure.book.ask_queue_quality = dec("0.18");
        strong.features.microstructure.book.ask_cancel_rate = dec("2400");
        strong.fill_quality.positive_markout_rate = dec("0.78");
        strong.fill_quality.adverse_selection_rate = dec("0.10");
        strong.fill_quality.fill_quality_score = dec("0.60");
        let strong_outcome = strategy.evaluate(&strong);
        let strong_intent = &strong_outcome.intents[0];

        assert!(strong_intent.quantity > normal_intent.quantity);
        assert_eq!(strong_intent.size_tier.as_deref(), Some("strong_signal_size"));
    }

    #[test]
    fn local_min_notional_floor_blocks_tiny_scalp_entry() {
        let mut context = sample_context();
        context.soft_inventory_base = dec("0.0020");
        context.max_inventory_base = dec("0.0050");
        context.local_min_notional_quote = dec("25.00");

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(outcome.entry_block_reason.as_deref(), Some("local_min_notional"));
    }

    #[test]
    fn slot_saturation_blocks_non_reduce_only_scalp_entry() {
        let mut context = sample_context();
        context.open_bot_orders_for_symbol = 4;
        context.max_open_orders_for_symbol = 4;

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(outcome.entry_block_reason.as_deref(), Some("slot_pressure"));
    }
}
