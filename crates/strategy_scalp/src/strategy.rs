use common::{Decimal, ids::new_id, now_utc};
use domain::{
    ExitStage, IntentRole, RegimeState, Side, StrategyContext, StrategyKind, StrategyOutcome,
    TimeInForce, TradeIntent,
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

#[derive(Debug, Clone, Copy)]
struct EntryAnalysis {
    entry_score: Decimal,
    momentum_signal_bps: Decimal,
    orderbook_signal: Decimal,
    bullish_pressure: Decimal,
    toxicity_pressure: Decimal,
    extension_penalty: Decimal,
    expected_edge_bps: Decimal,
    effective_edge_after_quality_bps: Decimal,
    required_edge_bps: Decimal,
    signal_quality: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct ExitAnalysis {
    pnl_bps: Decimal,
    tp_bps: Decimal,
    stop_bps: Decimal,
    invalidation_pressure: Decimal,
    inventory_pressure: Decimal,
    position_age_secs: i64,
    stale_inventory: bool,
    take_profit_ready: bool,
    capital_protection: bool,
    passive_viable: bool,
    aggressive_unwind: bool,
    exit_quantity: Decimal,
}

impl ScalpingStrategy {
    pub fn evaluate(&self, context: &StrategyContext) -> StrategyOutcome {
        let Some(best) = &context.best_bid_ask else {
            return standby("scalping waiting for best bid/ask");
        };

        if context.features.toxicity_score > self.config.max_toxicity_score {
            return standby("scalping blocked by toxicity");
        }

        if context.features.liquidity_score <= Decimal::ZERO
            || context.features.top_of_book_depth_quote <= Decimal::ZERO
        {
            return standby("scalping blocked by weak liquidity");
        }

        let max_position = context.max_inventory_base * self.config.max_position_fraction;
        let base_quote_size = (context.soft_inventory_base * self.config.quote_size_fraction).max(dec("0.0001"));
        let entry_cost_bps = self.config.taker_fee_bps + self.config.slippage_buffer_bps;
        let expires_at = Some(now_utc() + time::Duration::seconds(self.config.quote_ttl_secs));

        if context.inventory.base_position > Decimal::ZERO {
            if let Some(exit) =
                self.exit_intent(context, best, base_quote_size, max_position, entry_cost_bps, expires_at)
            {
                let best_expected_realized_edge_bps = Some(exit.expected_realized_edge_bps);
                let adverse_selection_hits =
                    u64::from(exit.adverse_selection_penalty_bps >= dec("0.30"));
                return StrategyOutcome {
                    intents: vec![exit],
                    standby_reason: None,
                    entry_block_reason: None,
                    best_expected_realized_edge_bps,
                    adverse_selection_hits,
                };
            }
        }

        if !matches!(context.regime.state, RegimeState::TrendUp | RegimeState::Range) {
            return standby("scalping entry disabled by regime");
        }

        if context.inventory.base_position >= max_position {
            return standby("scalping blocked by position cap");
        }

        if available_order_slots(context) == 0 {
            return standby("scalping blocked by symbol order-slot saturation");
        }

        let entry = analyze_long_entry(context, entry_cost_bps, &self.config);
        if entry.momentum_signal_bps < self.config.min_momentum_bps {
            return standby("scalping entry blocked by weak short momentum");
        }

        if context.features.trade_flow_imbalance <= dec("0.05") || entry.orderbook_signal <= dec("-0.05") {
            return standby("scalping entry blocked by weak directional confirmation");
        }

        if entry.extension_penalty >= dec("0.75") {
            return standby("scalping entry blocked by overextension");
        }

        if entry.entry_score < self.config.min_entry_score || entry.bullish_pressure < dec("0.20") {
            return standby("scalping entry score too weak");
        }

        if entry.effective_edge_after_quality_bps < entry.required_edge_bps {
            return standby("scalping edge is not positive enough after costs");
        }

        let quantity = entry_size(
            context,
            base_quote_size,
            max_position,
            entry.signal_quality,
            entry.toxicity_pressure,
            entry.effective_edge_after_quality_bps,
        );

        if quantity <= Decimal::ZERO {
            return standby("scalping size collapsed after inventory and signal filters");
        }

        if !has_deployable_notional(
            quantity,
            best.ask_price,
            context.local_min_notional_quote,
        ) {
            return standby("scalping size below local deployable notional floor");
        }

        StrategyOutcome {
            intents: vec![TradeIntent {
                intent_id: format!("scalp-{}", new_id()),
                symbol: context.symbol,
                strategy: StrategyKind::Scalping,
                side: Side::Buy,
                quantity,
                limit_price: Some(best.ask_price),
                max_slippage_bps: self.config.slippage_buffer_bps,
                post_only: false,
                reduce_only: false,
                time_in_force: Some(TimeInForce::Ioc),
                role: IntentRole::AddRisk,
                exit_stage: None,
                exit_reason: None,
                expected_edge_bps: entry.expected_edge_bps,
                expected_fee_bps: self.config.taker_fee_bps,
                expected_slippage_bps: self.config.slippage_buffer_bps,
                edge_after_cost_bps: entry.effective_edge_after_quality_bps,
                expected_realized_edge_bps: entry.effective_edge_after_quality_bps,
                adverse_selection_penalty_bps: Decimal::ZERO,
                reason: format!(
                    "scalp long entry: score={} pressure={} mom={} flow={} book={} vwap_dist={} tox={} required_edge_bps={} effective_edge_bps={} slots={}/{} size={}",
                    entry.entry_score,
                    entry.bullish_pressure,
                    entry.momentum_signal_bps,
                    context.features.trade_flow_imbalance,
                    entry.orderbook_signal,
                    context.features.vwap_distance_bps,
                    context.features.toxicity_score,
                    entry.required_edge_bps,
                    entry.effective_edge_after_quality_bps,
                    context.open_bot_orders_for_symbol,
                    context.max_open_orders_for_symbol,
                    quantity,
                ),
                created_at: now_utc(),
                expires_at,
            }],
            standby_reason: None,
            entry_block_reason: None,
            best_expected_realized_edge_bps: Some(entry.effective_edge_after_quality_bps),
            adverse_selection_hits: 0,
        }
    }

    fn exit_intent(
        &self,
        context: &StrategyContext,
        best: &domain::BestBidAsk,
        base_quote_size: Decimal,
        max_position: Decimal,
        entry_cost_bps: Decimal,
        expires_at: Option<common::Timestamp>,
    ) -> Option<TradeIntent> {
        let average_entry = context.inventory.average_entry_price?;
        if average_entry <= Decimal::ZERO {
            return None;
        }

        let exit = analyze_exit(context, &self.config, base_quote_size, max_position, average_entry, best);
        if !(exit.capital_protection
            || exit.take_profit_ready
            || exit.invalidation_pressure >= dec("0.60")
            || exit.stale_inventory)
        {
            return None;
        }

        let (role, exit_stage, post_only, time_in_force, limit_price) = if exit.aggressive_unwind {
            (
                if exit.position_age_secs >= self.config.aggressive_exit_secs {
                    IntentRole::EmergencyExit
                } else {
                    IntentRole::ForcedUnwind
                },
                Some(if exit.position_age_secs >= self.config.aggressive_exit_secs {
                    ExitStage::Emergency
                } else {
                    ExitStage::Aggressive
                }),
                false,
                Some(TimeInForce::Ioc),
                best.bid_price,
            )
        } else if exit.passive_viable {
            (
                if exit.take_profit_ready {
                    IntentRole::PassiveProfitTake
                } else {
                    IntentRole::DefensiveExit
                },
                Some(ExitStage::Passive),
                true,
                Some(TimeInForce::Gtc),
                best.ask_price,
            )
        } else {
            (
                IntentRole::DefensiveExit,
                Some(ExitStage::Tighten),
                true,
                Some(TimeInForce::Gtc),
                best.ask_price,
            )
        };

        let reason = if exit.capital_protection {
            format!(
                "scalp exit protect: pnl_bps={} stop_bps={} invalidation={} flow={} mom1={} age_secs={} regime={:?}",
                exit.pnl_bps,
                exit.stop_bps,
                exit.invalidation_pressure,
                context.features.trade_flow_imbalance,
                context.features.momentum_1s_bps,
                exit.position_age_secs,
                context.regime.state
            )
        } else if exit.take_profit_ready {
            format!(
                "scalp exit take-profit: pnl_bps={} tp_bps={} invalidation={} vwap_dist={} flow={} age_secs={}",
                exit.pnl_bps,
                exit.tp_bps,
                exit.invalidation_pressure,
                context.features.vwap_distance_bps,
                context.features.trade_flow_imbalance,
                exit.position_age_secs,
            )
        } else if exit.stale_inventory {
            format!(
                "scalp stale inventory exit: pnl_bps={} invalidation={} inventory_pressure={} age_secs={}",
                exit.pnl_bps,
                exit.invalidation_pressure,
                exit.inventory_pressure,
                exit.position_age_secs,
            )
        } else {
            format!(
                "scalp exit invalidation: pnl_bps={} invalidation={} flow={} book={} mom1={} age_secs={}",
                exit.pnl_bps,
                exit.invalidation_pressure,
                context.features.trade_flow_imbalance,
                orderbook_signal(context),
                context.features.momentum_1s_bps,
                exit.position_age_secs,
            )
        };

        Some(TradeIntent {
            intent_id: format!("scalp-exit-{}", new_id()),
            symbol: context.symbol,
            strategy: StrategyKind::Scalping,
            side: Side::Sell,
            quantity: exit.exit_quantity.min(context.inventory.base_position),
            limit_price: Some(limit_price),
            max_slippage_bps: self.config.slippage_buffer_bps,
            post_only,
            reduce_only: true,
            time_in_force,
            role,
            exit_stage,
            exit_reason: Some(reason.clone()),
            expected_edge_bps: exit.pnl_bps.abs().max(exit.tp_bps / dec("2.0")),
            expected_fee_bps: self.config.taker_fee_bps,
            expected_slippage_bps: self.config.slippage_buffer_bps,
            edge_after_cost_bps: exit.pnl_bps - if post_only {
                self.config.taker_fee_bps * dec("0.75")
            } else {
                entry_cost_bps
            },
            expected_realized_edge_bps: exit.pnl_bps - if post_only {
                self.config.taker_fee_bps * dec("0.75")
            } else {
                entry_cost_bps
            },
            adverse_selection_penalty_bps: Decimal::ZERO,
            reason,
            created_at: now_utc(),
            expires_at,
        })
    }
}

fn analyze_long_entry(context: &StrategyContext, entry_cost_bps: Decimal, config: &ScalpingConfig) -> EntryAnalysis {
    let momentum_signal_bps = short_momentum_signal_bps(context);
    let orderbook_signal = orderbook_signal(context);
    let bullish_pressure = bullish_pressure(context, momentum_signal_bps, orderbook_signal);
    let toxicity_pressure = normalized_score(context.features.toxicity_score, dec("0.20"), config.max_toxicity_score);
    let extension_penalty = positive_extension_penalty(context.features.vwap_distance_bps);
    let momentum_component = normalized_score(momentum_signal_bps, config.min_momentum_bps, dec("14"));
    let flow_component = normalized_score(context.features.trade_flow_imbalance, dec("0.05"), dec("0.35"));
    let book_component = normalized_score(orderbook_signal, dec("0.02"), dec("0.25"));
    let vwap_alignment = vwap_alignment_score(context.features.vwap_distance_bps);
    let liquidity_component = normalized_score(context.features.liquidity_score, dec("1500"), dec("7000"));
    let regime_component = match context.regime.state {
        RegimeState::TrendUp => Decimal::ONE,
        RegimeState::Range => dec("0.60"),
        _ => Decimal::ZERO,
    };

    let entry_score = momentum_component * dec("3.2")
        + flow_component * dec("2.7")
        + book_component * dec("1.6")
        + vwap_alignment * dec("1.0")
        + liquidity_component * dec("1.5")
        + regime_component * dec("1.5")
        - toxicity_pressure * dec("1.0")
        - extension_penalty * dec("1.25");

    let raw_alpha_bps = momentum_signal_bps * dec("0.45")
        + context.features.trade_flow_imbalance.max(Decimal::ZERO) * dec("12")
        + orderbook_signal.max(Decimal::ZERO) * dec("8")
        + vwap_alignment * dec("2.0")
        + regime_component * dec("0.8");
    let fragility_penalty_bps = toxicity_pressure * dec("2.5") + extension_penalty * dec("2.0");
    let expected_edge_bps = (raw_alpha_bps - fragility_penalty_bps).max(Decimal::ZERO);
    let edge_after_cost_bps = expected_edge_bps - entry_cost_bps;
    let slot_pressure = slot_pressure(context);
    let inventory_usage = if context.max_inventory_base <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        clamp_unit((context.inventory.base_position / context.max_inventory_base).max(Decimal::ZERO))
    };
    let quality_penalty_bps = toxicity_pressure * dec("0.95")
        + extension_penalty * dec("1.05")
        + (Decimal::ONE - bullish_pressure).max(Decimal::ZERO) * dec("0.85")
        + inventory_usage * dec("0.90")
        + slot_pressure * dec("0.70");
    let effective_edge_after_quality_bps = edge_after_cost_bps - quality_penalty_bps;
    let required_edge_bps = dec("1.50")
        + toxicity_pressure * dec("0.40")
        + extension_penalty * dec("0.35")
        + slot_pressure * dec("0.30");
    let signal_quality = clamp_unit((entry_score - config.min_entry_score + dec("1.0")) / dec("4.0"));

    EntryAnalysis {
        entry_score,
        momentum_signal_bps,
        orderbook_signal,
        bullish_pressure,
        toxicity_pressure,
        extension_penalty,
        expected_edge_bps,
        effective_edge_after_quality_bps,
        required_edge_bps,
        signal_quality,
    }
}

fn analyze_exit(
    context: &StrategyContext,
    config: &ScalpingConfig,
    base_quote_size: Decimal,
    max_position: Decimal,
    average_entry: Decimal,
    best: &domain::BestBidAsk,
) -> ExitAnalysis {
    let exit_price = best.bid_price;
    let pnl_bps = ((exit_price - average_entry) / average_entry) * Decimal::from(10_000u32);
    let momentum_signal_bps = short_momentum_signal_bps(context);
    let orderbook_signal = orderbook_signal(context);
    let negative_momentum = clamp_unit((-momentum_signal_bps / dec("10")).max(Decimal::ZERO));
    let negative_flow = clamp_unit(((-context.features.trade_flow_imbalance) / dec("0.25")).max(Decimal::ZERO));
    let negative_book = clamp_unit(((-orderbook_signal) / dec("0.20")).max(Decimal::ZERO));
    let toxicity_pressure = normalized_score(context.features.toxicity_score, dec("0.35"), dec("0.85"));
    let regime_exit = if matches!(context.regime.state, RegimeState::TrendDown) {
        dec("0.80")
    } else {
        Decimal::ZERO
    };
    let invalidation_pressure = clamp_unit(
        negative_momentum * dec("0.35")
            + negative_flow * dec("0.25")
            + negative_book * dec("0.20")
            + toxicity_pressure * dec("0.10")
            + regime_exit * dec("0.10"),
    );
    let inventory_pressure = if max_position <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        clamp_unit((context.inventory.base_position / max_position).max(Decimal::ZERO))
    };
    let position_age_secs = context
        .inventory
        .position_opened_at
        .map(|opened_at| (context.features.computed_at - opened_at).whole_seconds())
        .unwrap_or_default();
    let tp_bps = context.features.realized_volatility_bps.max(dec("4.0"));
    let stop_bps = (context.features.realized_volatility_bps * dec("0.60")).max(dec("3.0"));
    let take_profit_ready = pnl_bps >= tp_bps
        && (invalidation_pressure >= dec("0.25")
            || context.features.vwap_distance_bps >= dec("6.0")
            || context.features.momentum_1s_bps <= dec("1.0"));
    let edge_decay = position_age_secs >= config.soft_hold_secs
        && pnl_bps <= tp_bps / dec("2.0")
        && best.ask_price > Decimal::ZERO
        && ((best.ask_price - best.bid_price) / best.ask_price) * Decimal::from(10_000u32)
            <= dec("1.2");
    let stale_inventory = position_age_secs >= config.stale_hold_secs
        || (inventory_pressure >= dec("0.72") && position_age_secs >= config.soft_hold_secs);
    let capital_protection = pnl_bps <= -stop_bps
        || invalidation_pressure >= dec("0.80")
        || context.features.toxicity_score >= dec("0.80")
        || (edge_decay && inventory_pressure >= dec("0.55"));
    let aggressive_unwind = capital_protection
        || position_age_secs >= config.aggressive_exit_secs
        || (stale_inventory && invalidation_pressure >= dec("0.60"))
        || (negative_flow >= dec("0.75") && negative_book >= dec("0.60"));
    let passive_viable = (take_profit_ready || edge_decay || stale_inventory)
        && !aggressive_unwind
        && pnl_bps > -(stop_bps / dec("2.0"));

    let full_exit = aggressive_unwind || invalidation_pressure >= dec("0.70");
    let partial_exit_size = (base_quote_size * dec("1.20"))
        .max(context.inventory.base_position * dec("0.50"))
        .min(max_position.max(base_quote_size));
    let exit_quantity = if full_exit {
        context.inventory.base_position
    } else {
        partial_exit_size.min(context.inventory.base_position)
    };

    ExitAnalysis {
        pnl_bps,
        tp_bps,
        stop_bps,
        invalidation_pressure,
        inventory_pressure,
        position_age_secs,
        stale_inventory,
        take_profit_ready,
        capital_protection,
        passive_viable,
        aggressive_unwind,
        exit_quantity,
    }
}

fn entry_size(
    context: &StrategyContext,
    base_quote_size: Decimal,
    max_position: Decimal,
    signal_quality: Decimal,
    toxicity_pressure: Decimal,
    edge_after_cost_bps: Decimal,
) -> Decimal {
    let inventory_usage = if max_position <= Decimal::ZERO {
        Decimal::ONE
    } else {
        clamp_unit((context.inventory.base_position / max_position).max(Decimal::ZERO))
    };
    let regime_factor = match context.regime.state {
        RegimeState::TrendUp => Decimal::ONE,
        RegimeState::Range => dec("0.70"),
        _ => Decimal::ZERO,
    };
    let toxicity_factor = (Decimal::ONE - toxicity_pressure * dec("0.70")).max(dec("0.30"));
    let inventory_factor = (Decimal::ONE - inventory_usage * dec("0.75")).max(dec("0.20"));
    let edge_factor = clamp_unit(edge_after_cost_bps / dec("6.0")).max(dec("0.30"));
    let scaled = base_quote_size * regime_factor * signal_quality.max(dec("0.35")) * toxicity_factor * inventory_factor * edge_factor;
    scaled
        .max(dec("0.0001"))
        .min((max_position - context.inventory.base_position).max(Decimal::ZERO))
}

fn has_deployable_notional(quantity: Decimal, limit_price: Decimal, local_min_notional_quote: Decimal) -> bool {
    quantity > Decimal::ZERO
        && limit_price > Decimal::ZERO
        && (quantity * limit_price) >= local_min_notional_quote
}

fn slot_pressure(context: &StrategyContext) -> Decimal {
    if context.max_open_orders_for_symbol == 0 {
        Decimal::ZERO
    } else {
        clamp_unit(
            Decimal::from(context.open_bot_orders_for_symbol)
                / Decimal::from(context.max_open_orders_for_symbol),
        )
    }
}

fn available_order_slots(context: &StrategyContext) -> u32 {
    context
        .max_open_orders_for_symbol
        .saturating_sub(context.open_bot_orders_for_symbol)
}

fn short_momentum_signal_bps(context: &StrategyContext) -> Decimal {
    (context.features.local_momentum_bps * dec("0.40"))
        + (context.features.momentum_1s_bps * dec("0.35"))
        + (context.features.momentum_5s_bps * dec("0.25"))
}

fn orderbook_signal(context: &StrategyContext) -> Decimal {
    (context.features.orderbook_imbalance.unwrap_or(Decimal::ZERO) * dec("0.60"))
        + (context.features.orderbook_imbalance_rolling * dec("0.40"))
}

fn bullish_pressure(context: &StrategyContext, momentum_signal_bps: Decimal, orderbook_signal: Decimal) -> Decimal {
    let regime_bias = match context.regime.state {
        RegimeState::TrendUp => dec("0.15"),
        RegimeState::Range => dec("0.05"),
        _ => Decimal::ZERO,
    };
    clamp_unit(
        normalized_score(momentum_signal_bps, dec("3.0"), dec("12.0")) * dec("0.35")
            + normalized_score(context.features.trade_flow_imbalance, dec("0.05"), dec("0.35")) * dec("0.30")
            + normalized_score(orderbook_signal, dec("0.02"), dec("0.25")) * dec("0.20")
            + normalized_score(context.features.vwap_distance_bps, dec("0.0"), dec("6.0")) * dec("0.10")
            + regime_bias * dec("0.05"),
    )
}

fn positive_extension_penalty(vwap_distance_bps: Decimal) -> Decimal {
    normalized_score(vwap_distance_bps, dec("6.0"), dec("12.0"))
}

fn vwap_alignment_score(vwap_distance_bps: Decimal) -> Decimal {
    if vwap_distance_bps < Decimal::ZERO {
        Decimal::ZERO
    } else if vwap_distance_bps <= dec("6.0") {
        clamp_unit(vwap_distance_bps / dec("6.0"))
    } else {
        clamp_unit(Decimal::ONE - ((vwap_distance_bps - dec("6.0")) / dec("6.0")))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{
        BestBidAsk, FeatureSnapshot, InventorySnapshot, RegimeDecision, RiskMode, RuntimeState,
        Symbol, VolatilityRegime,
    };

    fn sample_context() -> StrategyContext {
        StrategyContext {
            symbol: Symbol::BtcUsdc,
            best_bid_ask: Some(BestBidAsk {
                symbol: Symbol::BtcUsdc,
                bid_price: dec("100.00"),
                bid_quantity: dec("2.0"),
                ask_price: dec("100.02"),
                ask_quantity: dec("2.0"),
                observed_at: now_utc(),
            }),
            features: FeatureSnapshot {
                symbol: Symbol::BtcUsdc,
                microprice: Some(dec("100.03")),
                top_of_book_depth_quote: dec("6000"),
                orderbook_imbalance: Some(dec("0.22")),
                orderbook_imbalance_rolling: dec("0.18"),
                realized_volatility_bps: dec("5.0"),
                vwap: dec("99.98"),
                vwap_distance_bps: dec("3.5"),
                spread_bps: dec("2.0"),
                spread_mean_bps: dec("1.6"),
                spread_std_bps: dec("0.2"),
                spread_zscore: dec("0.30"),
                trade_flow_imbalance: dec("0.28"),
                trade_flow_rate: dec("1.5"),
                trade_rate_per_sec: dec("2.0"),
                tick_rate_per_sec: dec("3.0"),
                tape_speed: dec("2.5"),
                momentum_1s_bps: dec("9.0"),
                momentum_5s_bps: dec("7.0"),
                momentum_15s_bps: dec("3.0"),
                local_momentum_bps: dec("10.0"),
                liquidity_score: dec("8200"),
                toxicity_score: dec("0.20"),
                volatility_regime: VolatilityRegime::Medium,
                computed_at: now_utc(),
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
                mark_price: Some(dec("100.01")),
                average_entry_price: None,
                position_opened_at: None,
                last_fill_at: None,
                first_reduce_at: None,
                updated_at: now_utc(),
            },
            soft_inventory_base: dec("0.020"),
            max_inventory_base: dec("0.050"),
            local_min_notional_quote: dec("0.01"),
            open_bot_orders_for_symbol: 0,
            max_open_orders_for_symbol: 4,
            runtime_state: RuntimeState::Trading,
            risk_mode: RiskMode::Normal,
        }
    }

    fn buy_quantity(outcome: &StrategyOutcome) -> Option<Decimal> {
        outcome
            .intents
            .iter()
            .find(|intent| intent.side == Side::Buy)
            .map(|intent| intent.quantity)
    }

    #[test]
    fn favorable_context_produces_valid_entry() {
        let outcome = ScalpingStrategy::default().evaluate(&sample_context());

        assert_eq!(outcome.intents.len(), 1);
        let intent = &outcome.intents[0];
        assert_eq!(intent.side, Side::Buy);
        assert!(intent.quantity > Decimal::ZERO);
        assert!(intent.edge_after_cost_bps >= dec("1.5"));
    }

    #[test]
    fn excessive_toxicity_blocks_scalping() {
        let mut context = sample_context();
        context.features.toxicity_score = dec("0.90");

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(outcome.standby_reason.as_deref(), Some("scalping blocked by toxicity"));
    }

    #[test]
    fn weak_signal_produces_no_entry() {
        let mut context = sample_context();
        context.features.local_momentum_bps = dec("2.0");
        context.features.momentum_1s_bps = dec("1.0");
        context.features.momentum_5s_bps = dec("1.0");
        context.features.trade_flow_imbalance = dec("0.03");
        context.features.orderbook_imbalance = Some(dec("0.01"));
        context.features.orderbook_imbalance_rolling = dec("0.00");

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert!(outcome.standby_reason.is_some());
    }

    #[test]
    fn clear_invalidation_triggers_reduce_only_exit() {
        let mut context = sample_context();
        context.inventory.base_position = dec("0.015");
        context.inventory.average_entry_price = Some(dec("100.10"));
        context.features.local_momentum_bps = dec("-8.0");
        context.features.momentum_1s_bps = dec("-9.0");
        context.features.momentum_5s_bps = dec("-6.0");
        context.features.trade_flow_imbalance = dec("-0.25");
        context.features.orderbook_imbalance = Some(dec("-0.22"));
        context.features.orderbook_imbalance_rolling = dec("-0.18");
        context.features.toxicity_score = dec("0.55");
        context.regime.state = RegimeState::TrendDown;

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert_eq!(outcome.intents.len(), 1);
        let intent = &outcome.intents[0];
        assert_eq!(intent.side, Side::Sell);
        assert!(intent.reduce_only);
        assert_eq!(intent.quantity, context.inventory.base_position);
    }

    #[test]
    fn weaker_but_valid_context_reduces_size_without_blocking() {
        let strategy = ScalpingStrategy::default();
        let healthy = strategy.evaluate(&sample_context());
        let healthy_qty = buy_quantity(&healthy).unwrap();

        let mut weaker = sample_context();
        weaker.features.local_momentum_bps = dec("7.5");
        weaker.features.momentum_1s_bps = dec("6.8");
        weaker.features.momentum_5s_bps = dec("5.5");
        weaker.features.trade_flow_imbalance = dec("0.20");
        weaker.features.orderbook_imbalance = Some(dec("0.15"));
        weaker.features.orderbook_imbalance_rolling = dec("0.12");
        weaker.features.toxicity_score = dec("0.50");
        weaker.inventory.base_position = dec("0.010");

        let weaker_outcome = strategy.evaluate(&weaker);
        let weaker_qty = buy_quantity(&weaker_outcome).unwrap();

        assert!(weaker_qty > Decimal::ZERO);
        assert!(weaker_qty < healthy_qty);
    }

    #[test]
    fn insufficient_edge_after_costs_blocks_entry() {
        let mut strategy = ScalpingStrategy::default();
        strategy.config.taker_fee_bps = dec("12.0");
        strategy.config.slippage_buffer_bps = dec("8.0");

        let outcome = strategy.evaluate(&sample_context());

        assert!(outcome.intents.is_empty());
        assert_eq!(
            outcome.standby_reason.as_deref(),
            Some("scalping edge is not positive enough after costs")
        );
    }

    #[test]
    fn local_min_notional_floor_blocks_tiny_scalp_entry() {
        let mut context = sample_context();
        context.local_min_notional_quote = dec("15.00");
        context.soft_inventory_base = dec("0.020");
        context.max_inventory_base = dec("0.050");

        let strategy = ScalpingStrategy {
            config: ScalpingConfig {
                quote_size_fraction: dec("0.03"),
                ..ScalpingConfig::default()
            },
        };

        let outcome = strategy.evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(
            outcome.standby_reason.as_deref(),
            Some("scalping size below local deployable notional floor")
        );
    }

    #[test]
    fn slot_saturation_blocks_non_reduce_only_scalp_entry() {
        let mut context = sample_context();
        context.open_bot_orders_for_symbol = 4;
        context.max_open_orders_for_symbol = 4;

        let outcome = ScalpingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(
            outcome.standby_reason.as_deref(),
            Some("scalping blocked by symbol order-slot saturation")
        );
    }
}
