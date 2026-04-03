use common::{Decimal, Timestamp, ids::new_id, now_utc};
use domain::{
    ExitStage, IntentRole, RegimeState, Side, StrategyContext, StrategyKind, StrategyOutcome,
    TimeInForce, TradeIntent,
};

#[derive(Debug, Clone)]
pub struct MarketMakingConfig {
    pub maker_fee_bps: Decimal,
    pub slippage_buffer_bps: Decimal,
    pub min_net_edge_bps: Decimal,
    pub min_market_spread_bps: Decimal,
    pub max_toxicity_score: Decimal,
    pub base_skew_bps: Decimal,
    pub momentum_skew_weight: Decimal,
    pub volatility_widening_weight: Decimal,
    pub quote_size_fraction: Decimal,
    pub quote_ttl_secs: i64,
    pub soft_hold_secs: i64,
    pub stale_hold_secs: i64,
    pub aggressive_exit_secs: i64,
}

impl Default for MarketMakingConfig {
    fn default() -> Self {
        Self {
            maker_fee_bps: Decimal::from_str_exact("0.75").unwrap(),
            slippage_buffer_bps: Decimal::from_str_exact("0.25").unwrap(),
            min_net_edge_bps: Decimal::from(1u32),
            min_market_spread_bps: Decimal::from_str_exact("0.80").unwrap(),
            max_toxicity_score: Decimal::from_str_exact("0.70").unwrap(),
            base_skew_bps: Decimal::from(8u32),
            momentum_skew_weight: Decimal::from_str_exact("0.25").unwrap(),
            volatility_widening_weight: Decimal::from_str_exact("0.30").unwrap(),
            quote_size_fraction: Decimal::from_str_exact("0.12").unwrap(),
            quote_ttl_secs: 5,
            soft_hold_secs: 20,
            stale_hold_secs: 45,
            aggressive_exit_secs: 75,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketMakingStrategy {
    pub config: MarketMakingConfig,
}

#[derive(Debug, Clone)]
struct CandidateIntent {
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    post_only: bool,
    time_in_force: Option<TimeInForce>,
    reduce_only: bool,
    role: IntentRole,
    exit_stage: Option<ExitStage>,
    exit_reason: Option<String>,
    expected_edge_bps: Decimal,
    edge_after_cost_bps: Decimal,
    expected_realized_edge_bps: Decimal,
    adverse_selection_penalty_bps: Decimal,
    priority_score: Decimal,
    reason: String,
}

impl MarketMakingStrategy {
    pub fn evaluate(&self, context: &StrategyContext) -> StrategyOutcome {
        if !matches!(
            context.regime.state,
            RegimeState::Range | RegimeState::TrendUp | RegimeState::TrendDown
        ) {
            return standby("market making disabled by regime");
        }

        let Some(best) = &context.best_bid_ask else {
            return standby("market making waiting for best bid/ask");
        };

        if context.features.liquidity_score <= Decimal::ZERO
            || context.features.top_of_book_depth_quote <= Decimal::ZERO
        {
            return standby("liquidity too weak for market making");
        }

        let available_slots = available_order_slots(context);
        if available_slots == 0 {
            return standby("market making withheld: symbol order slots saturated");
        }

        let current_time = context.features.computed_at;
        let mid = (best.bid_price + best.ask_price) / Decimal::from(2u32);
        let fair = context.features.microprice.unwrap_or(mid);
        let inventory_ratio = inventory_ratio(context);
        let neutral_inventory = inventory_ratio.abs() < dec("0.05");
        let positive_inventory_ratio = clamp_unit(inventory_ratio.max(Decimal::ZERO));
        let slot_pressure = slot_pressure(context);
        let position_age_secs = position_age_secs(context, current_time);
        let orderbook_signal = orderbook_signal(context);
        let momentum_signal_bps = short_momentum_signal_bps(context);
        let directional_pressure =
            directional_pressure_score(context, momentum_signal_bps, orderbook_signal);
        let toxicity_pressure =
            normalized_score(context.features.toxicity_score, dec("0.25"), self.config.max_toxicity_score);
        let flow_book_pressure = max_decimal(
            normalized_abs_score(context.features.trade_flow_imbalance, dec("0.45")),
            normalized_abs_score(orderbook_signal, dec("0.35")),
        );
        let vwap_extension = normalized_abs_score(context.features.vwap_distance_bps, dec("8"));
        let momentum_extension = normalized_abs_score(momentum_signal_bps, dec("12"));
        let volatility_pressure =
            normalized_score(context.features.realized_volatility_bps, dec("1.5"), dec("8.0"));
        let toxic_mode = context.features.toxicity_score > self.config.max_toxicity_score;
        let entry_ttl_secs = if toxic_mode {
            self.config.quote_ttl_secs + 2
        } else if toxicity_pressure >= dec("0.65") {
            self.config.quote_ttl_secs + 1
        } else {
            self.config.quote_ttl_secs
        };

        let inventory_skew_bps = -inventory_ratio * self.config.base_skew_bps;
        let directional_center_skew_bps =
            directional_pressure * self.config.base_skew_bps * self.config.momentum_skew_weight;
        let mean_reversion_skew_bps =
            clamp_signed_bps(-context.features.vwap_distance_bps * dec("0.30"), dec("4"));
        let quote_center = shift_price_bps(
            fair,
            inventory_skew_bps + directional_center_skew_bps + mean_reversion_skew_bps,
        );

        let base_required_edge_bps = self.config.min_net_edge_bps.max(dec("0.65"));
        let minimum_required_edge_bps =
            self.config.maker_fee_bps + self.config.slippage_buffer_bps + base_required_edge_bps.min(dec("0.85"));
        let volatility_widening_bps =
            context.features.realized_volatility_bps * (self.config.volatility_widening_weight * dec("0.55"));
        let toxicity_widening_bps = if toxic_mode {
            dec("0.25") + toxicity_pressure * dec("0.30")
        } else {
            toxicity_pressure * dec("0.18")
        };
        let directional_widening_bps = directional_pressure.abs() * dec("0.35");
        let imbalance_widening_bps = flow_book_pressure * dec("0.28");
        let extension_widening_bps = (vwap_extension * dec("0.18")) + (momentum_extension * dec("0.10"));
        let dynamic_half_spread_bps = (context.features.spread_bps / Decimal::from(2u32)).max(
            minimum_required_edge_bps
                + volatility_widening_bps
                + toxicity_widening_bps
                + directional_widening_bps
                + imbalance_widening_bps
                + extension_widening_bps,
        );

        let reduce_only_trigger = context.max_inventory_base * dec("0.90");
        let inventory_add_risk_blocked = context.inventory.base_position.abs() >= context.max_inventory_base
            || context.inventory.base_position >= reduce_only_trigger;
        let add_risk_block_reason = if inventory_add_risk_blocked {
            Some("inventory".to_string())
        } else {
            None
        };
        let add_risk_width_viable = dynamic_half_spread_bps < dec("14.0");
        let raw_buy_price = shift_price_bps(quote_center, -dynamic_half_spread_bps);
        let raw_sell_price = shift_price_bps(quote_center, dynamic_half_spread_bps);
        let buy_price = raw_buy_price.min(best.bid_price);
        let sell_price = raw_sell_price.max(best.ask_price);

        let bid_expected_edge_bps = edge_from_fair_bps(fair, buy_price, Side::Buy);
        let ask_expected_edge_bps = edge_from_fair_bps(fair, sell_price, Side::Sell);
        let bid_edge_after_cost_bps =
            bid_expected_edge_bps - self.config.maker_fee_bps - self.config.slippage_buffer_bps;
        let ask_edge_after_cost_bps =
            ask_expected_edge_bps - self.config.maker_fee_bps - self.config.slippage_buffer_bps;

        let bearish_pressure = clamp_unit((-directional_pressure).max(Decimal::ZERO));
        let bullish_pressure = clamp_unit(directional_pressure.max(Decimal::ZERO));
        let bid_adverse_selection_penalty_bps = buy_adverse_selection_penalty_bps(
            context,
            toxicity_pressure,
            positive_inventory_ratio,
            slot_pressure,
        );
        let bid_flow_penalty_bps = buy_flow_penalty_bps(context);
        let ask_adverse_selection_penalty_bps = bullish_pressure * dec("0.30")
            + toxicity_pressure * dec("0.12")
            + flow_book_pressure * dec("0.08")
            + vwap_extension * dec("0.05")
            + slot_pressure * dec("0.08");
        let bid_hazard_penalty_bps = (bid_hazard_score(
            bearish_pressure,
            toxicity_pressure,
            flow_book_pressure,
            vwap_extension,
        ) * dec("0.50"))
        .min(dec("0.50"));
        let ask_hazard_penalty_bps = (ask_hazard_score(
            bullish_pressure,
            toxicity_pressure,
            flow_book_pressure,
            vwap_extension,
        ) * dec("0.50"))
        .min(dec("0.50"));
        let bid_effective_edge_bps =
            bid_edge_after_cost_bps - bid_adverse_selection_penalty_bps - bid_hazard_penalty_bps;
        let bid_expected_realized_edge_bps = bid_effective_edge_bps - bid_flow_penalty_bps;
        let ask_effective_edge_bps =
            ask_edge_after_cost_bps - ask_adverse_selection_penalty_bps - ask_hazard_penalty_bps
                + positive_inventory_ratio * dec("0.35");
        let bid_hazard = bid_hazard_score(
            bearish_pressure,
            toxicity_pressure,
            flow_book_pressure,
            vwap_extension,
        );
        let ask_hazard = ask_hazard_score(
            bullish_pressure,
            toxicity_pressure,
            flow_book_pressure,
            vwap_extension,
        );

        let regime_size_factor = match context.regime.state {
            RegimeState::Range => Decimal::ONE,
            RegimeState::TrendUp | RegimeState::TrendDown => dec("0.70"),
            _ => Decimal::ZERO,
        };
        let toxicity_size_factor = (Decimal::ONE - toxicity_pressure * dec("0.65")).max(dec("0.25"));
        let base_quote_size = context.soft_inventory_base * self.config.quote_size_fraction;
        let bid_edge_size_factor = clamp_unit(bid_edge_after_cost_bps.max(Decimal::ZERO) / dec("6")).max(dec("0.25"));
        let ask_edge_size_factor = clamp_unit(ask_edge_after_cost_bps.max(Decimal::ZERO) / dec("6")).max(dec("0.25"));
        let bid_inventory_size_factor =
            (Decimal::ONE - positive_inventory_ratio * dec("1.05")).max(dec("0.15"));
        let ask_inventory_size_factor = if positive_inventory_ratio > Decimal::ZERO {
            (dec("0.85") + positive_inventory_ratio * dec("0.35")).min(dec("1.10"))
        } else {
            Decimal::ZERO
        };

        let bid_size = scaled_quote_size(
            base_quote_size,
            regime_size_factor * toxicity_size_factor * bid_edge_size_factor * bid_inventory_size_factor,
        );
        let ask_size = scaled_quote_size(
            base_quote_size,
            regime_size_factor * toxicity_size_factor * ask_edge_size_factor * ask_inventory_size_factor,
        );

        let bid_required_edge_bps = capped_entry_required_edge_bps(
            context.symbol,
            base_required_edge_bps,
            toxicity_pressure * dec("0.35"),
            positive_inventory_ratio * dec("0.30"),
            volatility_pressure * dec("0.20"),
            neutral_inventory,
        );
        let ask_required_edge_bps = capped_entry_required_edge_bps(
            context.symbol,
            base_required_edge_bps.max(dec("0.55")),
            toxicity_pressure * dec("0.20"),
            Decimal::ZERO,
            volatility_pressure * dec("0.12"),
            neutral_inventory,
        );
        let ask_required_edge_bps =
            (ask_required_edge_bps - positive_inventory_ratio * dec("0.18")).max(dec("0.45"));
        let bid_hazard_excessive = bid_hazard >= dec("0.96");
        let ask_hazard_excessive = ask_hazard >= dec("0.98");
        let bid_deployable = has_deployable_notional(
            bid_size,
            buy_price,
            context.local_min_notional_quote,
        );
        let ask_deployable = has_deployable_notional(
            ask_size.min(context.inventory.base_position),
            sell_price,
            context.local_min_notional_quote,
        );

        let bid_allowed = add_risk_block_reason.is_none()
            && add_risk_width_viable
            && context.inventory.base_position <= reduce_only_trigger
            && !bid_hazard_excessive
            && (!toxic_mode || bid_effective_edge_bps >= bid_required_edge_bps + dec("0.60"))
            && bid_effective_edge_bps >= bid_required_edge_bps
            && bid_expected_realized_edge_bps > dec("0.30")
            && bid_size > Decimal::ZERO
            && bid_deployable;
        let ask_direction_ok = bullish_pressure < dec("0.75") || positive_inventory_ratio > dec("0.35");
        let ask_allowed = context.inventory.base_position > Decimal::ZERO
            && ask_direction_ok
            && !ask_hazard_excessive
            && ask_effective_edge_bps >= ask_required_edge_bps
            && ask_size > Decimal::ZERO
            && ask_deployable;
        let probe_bid_expected_edge_bps = edge_from_fair_bps(fair, best.bid_price, Side::Buy);
        let probe_bid_edge_after_cost_bps = probe_bid_expected_edge_bps
            - self.config.maker_fee_bps
            - self.config.slippage_buffer_bps
            - bid_adverse_selection_penalty_bps.min(dec("0.35"))
            - bid_hazard_penalty_bps;
        let probe_bid_expected_realized_edge_bps = probe_bid_edge_after_cost_bps - bid_flow_penalty_bps;
        let probe_threshold_bps = probing_edge_threshold(context.symbol);
        let probe_width_viable = dynamic_half_spread_bps < dec("8.0");
        let probe_size_fraction = ((dec("0.10")
            + clamp_unit(
                (probe_bid_edge_after_cost_bps - probe_threshold_bps).max(Decimal::ZERO) / dec("1.00"),
            ) * dec("0.15"))
            * if toxic_mode { dec("0.55") } else { Decimal::ONE })
        .min(dec("0.25"));
        let bid_probe_size = scaled_quote_size(
            bid_size.max(base_quote_size * dec("0.25")),
            probe_size_fraction,
        );
        let bid_probe_deployable = has_deployable_notional(
            bid_probe_size,
            best.bid_price,
            context.local_min_notional_quote,
        );
        let expires_at = Some(now_utc() + time::Duration::seconds(entry_ttl_secs));
        let mut candidates = Vec::new();
        let mut withheld = Vec::new();

        if let Some(reason) = &add_risk_block_reason {
            withheld.push(reason.clone());
        }
        if !add_risk_width_viable {
            withheld.push("required passive edge exceeds deployable passive quoting width".to_string());
        }
        let exit_candidate = staged_exit_candidate(
            self,
            context,
            best,
            fair,
            current_time,
            position_age_secs,
            positive_inventory_ratio,
            slot_pressure,
            bearish_pressure,
            toxicity_pressure,
            flow_book_pressure,
            vwap_extension,
            ask_effective_edge_bps,
            ask_required_edge_bps,
            dynamic_half_spread_bps,
        );
        let exit_blocks_new_risk = exit_candidate
            .as_ref()
            .map(|candidate| {
                matches!(
                    candidate.role,
                    IntentRole::ForcedUnwind | IntentRole::EmergencyExit
                ) || matches!(candidate.exit_stage, Some(ExitStage::Aggressive | ExitStage::Emergency))
            })
            .unwrap_or(false);
        let bid_probe_allowed = add_risk_block_reason.is_none()
            && !exit_blocks_new_risk
            && probe_width_viable
            && bearish_pressure < dec("0.35")
            && flow_book_pressure < dec("0.55")
            && context.features.local_momentum_bps >= dec("-0.5")
            && context.features.trade_flow_imbalance >= dec("-0.5")
            && !bid_hazard_excessive
            && probe_bid_edge_after_cost_bps > probe_threshold_bps
            && probe_bid_expected_realized_edge_bps > dec("0.30")
            && bid_probe_size > Decimal::ZERO
            && bid_probe_deployable;

        if let Some(candidate) = exit_candidate {
            candidates.push(candidate);
        }

        if bid_allowed {
            if !exit_blocks_new_risk {
                candidates.push(CandidateIntent {
                    side: Side::Buy,
                    quantity: bid_size,
                    limit_price: buy_price,
                    post_only: true,
                    time_in_force: Some(TimeInForce::Gtc),
                    reduce_only: false,
                    role: IntentRole::AddRisk,
                    exit_stage: None,
                    exit_reason: None,
                    expected_edge_bps: bid_expected_edge_bps,
                    edge_after_cost_bps: bid_effective_edge_bps,
                    expected_realized_edge_bps: bid_expected_realized_edge_bps,
                    adverse_selection_penalty_bps: bid_adverse_selection_penalty_bps
                        + bid_flow_penalty_bps,
                    priority_score: bid_effective_edge_bps,
                    reason: format!(
                        "mm bid: fair={} inv_skew_bps={} pressure_skew_bps={} mean_rev_skew_bps={} half_spread_bps={} bid_hazard={} bid_required_edge_bps={} bid_effective_edge_bps={} expected_realized_edge_bps={} adverse_penalty_bps={} flow_penalty_bps={} dir_pressure={} tox={} slot_pressure={} age_secs={} size={}",
                        fair,
                        inventory_skew_bps,
                        directional_center_skew_bps,
                        mean_reversion_skew_bps,
                        dynamic_half_spread_bps,
                        bid_hazard,
                        bid_required_edge_bps,
                        bid_effective_edge_bps,
                        bid_expected_realized_edge_bps,
                        bid_adverse_selection_penalty_bps,
                        bid_flow_penalty_bps,
                        directional_pressure,
                        context.features.toxicity_score,
                        slot_pressure,
                        position_age_secs.unwrap_or_default(),
                        bid_size,
                    ),
                });
            } else {
                withheld.push("bid suppressed while forced reduce-risk unwind is active".to_string());
            }
        } else if bid_probe_allowed {
            candidates.push(CandidateIntent {
                side: Side::Buy,
                quantity: bid_probe_size,
                limit_price: best.bid_price,
                post_only: true,
                time_in_force: Some(TimeInForce::Gtc),
                reduce_only: false,
                role: IntentRole::AddRisk,
                exit_stage: None,
                exit_reason: None,
                expected_edge_bps: probe_bid_expected_edge_bps,
                edge_after_cost_bps: probe_bid_edge_after_cost_bps,
                expected_realized_edge_bps: probe_bid_expected_realized_edge_bps,
                adverse_selection_penalty_bps: bid_adverse_selection_penalty_bps.min(dec("0.35"))
                    + bid_flow_penalty_bps,
                priority_score: probe_bid_expected_realized_edge_bps * dec("0.75"),
                reason: format!(
                    "mm probing bid: fair={} probe_edge_after_cost_bps={} probe_expected_realized_edge_bps={} required_edge_bps={} tox={} hazard={} adverse_penalty_bps={} flow_penalty_bps={} size={} ttl_secs={}",
                    fair,
                    probe_bid_edge_after_cost_bps,
                    probe_bid_expected_realized_edge_bps,
                    bid_required_edge_bps,
                    context.features.toxicity_score,
                    bid_hazard,
                    bid_adverse_selection_penalty_bps.min(dec("0.35")),
                    bid_flow_penalty_bps,
                    bid_probe_size,
                    entry_ttl_secs,
                ),
            });
        } else {
            withheld.push(format!(
                "bid gated: edge_after_cost_bps={} effective_edge_bps={} required_edge_bps={} hazard={} deployable={} inv_ratio={} slots={}/{}",
                bid_edge_after_cost_bps,
                bid_effective_edge_bps,
                bid_required_edge_bps,
                bid_hazard,
                bid_deployable,
                inventory_ratio,
                context.open_bot_orders_for_symbol,
                context.max_open_orders_for_symbol,
            ));
        }

        if ask_allowed {
            let reduce_risk = context.inventory.base_position.abs() >= reduce_only_trigger;
            candidates.push(CandidateIntent {
                side: Side::Sell,
                quantity: ask_size.min(context.inventory.base_position),
                limit_price: sell_price,
                post_only: true,
                time_in_force: Some(TimeInForce::Gtc),
                reduce_only: reduce_risk,
                role: if reduce_risk {
                    IntentRole::ReduceRisk
                } else {
                    IntentRole::PassiveProfitTake
                },
                exit_stage: if reduce_risk { Some(ExitStage::Passive) } else { None },
                exit_reason: if reduce_risk {
                    Some("inventory pressure passive ask".to_string())
                } else {
                    None
                },
                expected_edge_bps: ask_expected_edge_bps,
                edge_after_cost_bps: ask_effective_edge_bps,
                expected_realized_edge_bps: ask_effective_edge_bps,
                adverse_selection_penalty_bps: ask_adverse_selection_penalty_bps,
                priority_score: ask_effective_edge_bps + if reduce_risk { dec("0.90") } else { dec("0.45") },
                reason: format!(
                    "mm ask: fair={} inv_skew_bps={} pressure_skew_bps={} mean_rev_skew_bps={} half_spread_bps={} ask_hazard={} ask_required_edge_bps={} ask_effective_edge_bps={} dir_pressure={} tox={} slot_pressure={} age_secs={} size={}",
                    fair,
                    inventory_skew_bps,
                    directional_center_skew_bps,
                    mean_reversion_skew_bps,
                    dynamic_half_spread_bps,
                    ask_hazard,
                    ask_required_edge_bps,
                    ask_effective_edge_bps,
                    directional_pressure,
                    context.features.toxicity_score,
                    slot_pressure,
                    position_age_secs.unwrap_or_default(),
                    ask_size.min(context.inventory.base_position),
                ),
            });
        } else if context.inventory.base_position > Decimal::ZERO {
            withheld.push(format!(
                "ask gated: edge_after_cost_bps={} effective_edge_bps={} required_edge_bps={} hazard={} deployable={} inv_ratio={} slots={}/{}",
                ask_edge_after_cost_bps,
                ask_effective_edge_bps,
                ask_required_edge_bps,
                ask_hazard,
                ask_deployable,
                inventory_ratio,
                context.open_bot_orders_for_symbol,
                context.max_open_orders_for_symbol,
            ));
        }

        if available_slots == 1 && candidates.len() > 1 {
            let keep_side = best_candidate_side(&candidates);
            candidates.retain(|candidate| candidate.side == keep_side);
            withheld.push("single remaining order slot reserved for highest-quality quote".to_string());
        }

        let has_add_risk_candidate = candidates
            .iter()
            .any(|candidate| matches!(candidate.role, IntentRole::AddRisk));
        let intents = candidates
            .into_iter()
            .take(available_slots as usize)
            .map(|candidate| {
                build_intent(
                    context,
                    candidate.side,
                    candidate.quantity,
                    candidate.limit_price,
                    candidate.post_only,
                    candidate.time_in_force,
                    candidate.reduce_only,
                    candidate.role,
                    candidate.exit_stage,
                    candidate.exit_reason,
                    candidate.expected_edge_bps,
                    self.config.maker_fee_bps,
                    self.config.slippage_buffer_bps,
                    candidate.edge_after_cost_bps,
                    candidate.expected_realized_edge_bps,
                    candidate.adverse_selection_penalty_bps,
                    candidate.reason,
                    expires_at,
                )
            })
            .collect::<Vec<_>>();

        let entry_block_reason = if has_add_risk_candidate {
            None
        } else if add_risk_block_reason.is_some() || exit_blocks_new_risk {
            Some("inventory".to_string())
        } else if toxic_mode && context.features.toxicity_score > self.config.max_toxicity_score {
            Some("toxicity".to_string())
        } else if bid_hazard_excessive {
            Some("hazard".to_string())
        } else {
            Some("edge_too_low".to_string())
        };
        let best_expected_realized_edge_bps =
            intents.iter().map(|intent| intent.expected_realized_edge_bps).max();
        let adverse_selection_hits = intents
            .iter()
            .filter(|intent| intent.adverse_selection_penalty_bps >= dec("0.30"))
            .count() as u64;

        if intents.is_empty() {
            StrategyOutcome {
                intents,
                standby_reason: Some(format!("market making withheld: {}", withheld.join("; "))),
                entry_block_reason,
                best_expected_realized_edge_bps,
                adverse_selection_hits,
            }
        } else {
            StrategyOutcome {
                intents,
                standby_reason: None,
                entry_block_reason,
                best_expected_realized_edge_bps,
                adverse_selection_hits,
            }
        }
    }
}

fn build_intent(
    context: &StrategyContext,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    post_only: bool,
    time_in_force: Option<TimeInForce>,
    reduce_only: bool,
    role: IntentRole,
    exit_stage: Option<ExitStage>,
    exit_reason: Option<String>,
    expected_edge_bps: Decimal,
    expected_fee_bps: Decimal,
    expected_slippage_bps: Decimal,
    edge_after_cost_bps: Decimal,
    expected_realized_edge_bps: Decimal,
    adverse_selection_penalty_bps: Decimal,
    reason: String,
    expires_at: Option<common::Timestamp>,
) -> TradeIntent {
    TradeIntent {
        intent_id: format!("mm-{}", new_id()),
        symbol: context.symbol,
        strategy: StrategyKind::MarketMaking,
        side,
        quantity,
        limit_price: Some(limit_price),
        max_slippage_bps: Decimal::from(2u32),
        post_only,
        reduce_only,
        time_in_force,
        role,
        exit_stage,
        exit_reason,
        expected_edge_bps,
        expected_fee_bps,
        expected_slippage_bps,
        edge_after_cost_bps,
        expected_realized_edge_bps,
        adverse_selection_penalty_bps,
        reason,
        created_at: now_utc(),
        expires_at,
    }
}

#[allow(clippy::too_many_arguments)]
fn staged_exit_candidate(
    strategy: &MarketMakingStrategy,
    context: &StrategyContext,
    best: &domain::BestBidAsk,
    fair: Decimal,
    _current_time: Timestamp,
    position_age_secs: Option<i64>,
    positive_inventory_ratio: Decimal,
    slot_pressure: Decimal,
    bearish_pressure: Decimal,
    toxicity_pressure: Decimal,
    flow_book_pressure: Decimal,
    vwap_extension: Decimal,
    ask_effective_edge_bps: Decimal,
    ask_required_edge_bps: Decimal,
    dynamic_half_spread_bps: Decimal,
) -> Option<CandidateIntent> {
    let base_position = context.inventory.base_position;
    if base_position <= Decimal::ZERO {
        return None;
    }

    let average_entry = context.inventory.average_entry_price?;
    let age_secs = position_age_secs.unwrap_or_default();
    let profit_bps = if average_entry > Decimal::ZERO {
        ((best.ask_price - average_entry) / average_entry) * Decimal::from(10_000u32)
    } else {
        Decimal::ZERO
    };
    let edge_dead = ask_effective_edge_bps < ask_required_edge_bps;
    let reversal_pressure = clamp_unit(
        bearish_pressure * dec("0.45")
            + flow_book_pressure * dec("0.25")
            + toxicity_pressure * dec("0.20")
            + vwap_extension * dec("0.10"),
    );
    let inventory_pressure = clamp_unit(
        positive_inventory_ratio * dec("0.65") + slot_pressure * dec("0.35"),
    );
    let stale_inventory =
        age_secs >= strategy.config.stale_hold_secs || inventory_pressure >= dec("0.72");
    let aggressive_unwind = age_secs >= strategy.config.aggressive_exit_secs
        || reversal_pressure >= dec("0.82")
        || (toxicity_pressure >= dec("0.75") && profit_bps <= Decimal::ZERO);
    let timeout_exit = age_secs >= strategy.config.soft_hold_secs && edge_dead;
    let profit_capture_ready = profit_bps
        >= (strategy.config.min_net_edge_bps + strategy.config.maker_fee_bps + dec("0.40"))
            .max(dynamic_half_spread_bps * dec("0.55"));

    if !(timeout_exit
        || stale_inventory
        || aggressive_unwind
        || profit_capture_ready
        || reversal_pressure >= dec("0.58"))
    {
        return None;
    }

    let (role, exit_stage, post_only, time_in_force, limit_price, exit_quantity, priority_boost, exit_reason) =
        if aggressive_unwind {
            (
                if age_secs >= strategy.config.aggressive_exit_secs {
                    IntentRole::EmergencyExit
                } else {
                    IntentRole::ForcedUnwind
                },
                Some(if age_secs >= strategy.config.aggressive_exit_secs {
                    ExitStage::Emergency
                } else {
                    ExitStage::Aggressive
                }),
                false,
                Some(TimeInForce::Ioc),
                best.bid_price,
                base_position,
                dec("4.5"),
                if age_secs >= strategy.config.aggressive_exit_secs {
                    "market making emergency exit: position over max hold and must be flattened"
                } else {
                    "market making aggressive unwind: reversal/toxicity pressure exceeded passive tolerance"
                }
                .to_string(),
            )
        } else if stale_inventory || timeout_exit || reversal_pressure >= dec("0.58") {
            (
                IntentRole::DefensiveExit,
                Some(ExitStage::Tighten),
                true,
                Some(TimeInForce::Gtc),
                best.ask_price,
                (base_position * dec("0.70")).max(context.soft_inventory_base * dec("0.45")).min(base_position),
                dec("2.4"),
                if stale_inventory {
                    "market making stale inventory exit: passive ask tightened to accelerate recycle"
                } else if timeout_exit {
                    "market making edge decay exit: hold time exceeded with degraded edge"
                } else {
                    "market making reversal exit: passive ask tightened after bearish reversal"
                }
                .to_string(),
            )
        } else {
            (
                IntentRole::PassiveProfitTake,
                Some(ExitStage::Passive),
                true,
                Some(TimeInForce::Gtc),
                best.ask_price.max(shift_price_bps(fair, dec("0.05"))),
                (base_position * dec("0.45")).max(context.soft_inventory_base * dec("0.30")).min(base_position),
                dec("1.5"),
                "market making passive profit-take: recycle inventory while edge remains positive"
                    .to_string(),
            )
        };

    if !has_deployable_notional(exit_quantity, limit_price, context.local_min_notional_quote) {
        return None;
    }

    Some(CandidateIntent {
        side: Side::Sell,
        quantity: exit_quantity,
        limit_price,
        post_only,
        time_in_force,
        reduce_only: true,
        role,
        exit_stage,
        exit_reason: Some(exit_reason.clone()),
        expected_edge_bps: profit_bps.max(Decimal::ZERO),
        edge_after_cost_bps: profit_bps - if post_only {
            strategy.config.maker_fee_bps
        } else {
            strategy.config.maker_fee_bps + strategy.config.slippage_buffer_bps + dec("0.50")
        },
        expected_realized_edge_bps: profit_bps - if post_only {
            strategy.config.maker_fee_bps
        } else {
            strategy.config.maker_fee_bps + strategy.config.slippage_buffer_bps + dec("0.50")
        },
        adverse_selection_penalty_bps: Decimal::ZERO,
        priority_score: profit_bps + priority_boost + inventory_pressure * dec("2.20"),
        reason: format!(
            "{} | age_secs={} profit_bps={} reversal_pressure={} inventory_pressure={} ask_effective_edge_bps={}",
            exit_reason,
            age_secs,
            profit_bps,
            reversal_pressure,
            inventory_pressure,
            ask_effective_edge_bps,
        ),
    })
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

fn shift_price_bps(price: Decimal, bps: Decimal) -> Decimal {
    if price.is_zero() {
        price
    } else {
        let shifted = price * (Decimal::ONE + bps / Decimal::from(10_000u32));
        shifted.max(Decimal::ZERO)
    }
}

fn edge_from_fair_bps(fair: Decimal, quote_price: Decimal, side: Side) -> Decimal {
    if fair <= Decimal::ZERO || quote_price <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    let raw_edge = match side {
        Side::Buy => fair - quote_price,
        Side::Sell => quote_price - fair,
    };

    if raw_edge <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (raw_edge / fair) * Decimal::from(10_000u32)
    }
}

fn inventory_ratio(context: &StrategyContext) -> Decimal {
    if context.max_inventory_base.is_zero() {
        Decimal::ZERO
    } else {
        context.inventory.base_position / context.max_inventory_base
    }
}

fn required_edge_cap(symbol: domain::Symbol) -> Decimal {
    match symbol {
        domain::Symbol::BtcUsdc => dec("1.4"),
        domain::Symbol::EthUsdc => dec("1.6"),
    }
}

fn probing_edge_threshold(symbol: domain::Symbol) -> Decimal {
    match symbol {
        domain::Symbol::BtcUsdc => dec("0.7"),
        domain::Symbol::EthUsdc => dec("0.9"),
    }
}

fn capped_entry_required_edge_bps(
    symbol: domain::Symbol,
    base_edge_bps: Decimal,
    toxicity_penalty_bps: Decimal,
    inventory_penalty_bps: Decimal,
    volatility_penalty_bps: Decimal,
    neutral_inventory: bool,
) -> Decimal {
    let mut required = base_edge_bps + toxicity_penalty_bps + inventory_penalty_bps + volatility_penalty_bps;
    if neutral_inventory {
        required *= dec("0.70");
    }
    required.min(required_edge_cap(symbol)).max(dec("0.45"))
}

fn position_age_secs(context: &StrategyContext, current_time: Timestamp) -> Option<i64> {
    context
        .inventory
        .position_opened_at
        .map(|opened_at| (current_time - opened_at).whole_seconds())
}

fn short_momentum_signal_bps(context: &StrategyContext) -> Decimal {
    (context.features.local_momentum_bps * dec("0.50"))
        + (context.features.momentum_1s_bps * dec("0.30"))
        + (context.features.momentum_5s_bps * dec("0.20"))
}

fn orderbook_signal(context: &StrategyContext) -> Decimal {
    (context.features.orderbook_imbalance.unwrap_or(Decimal::ZERO) * dec("0.60"))
        + (context.features.orderbook_imbalance_rolling * dec("0.40"))
}

fn directional_pressure_score(
    context: &StrategyContext,
    momentum_signal_bps: Decimal,
    orderbook_signal: Decimal,
) -> Decimal {
    let regime_bias = match context.regime.state {
        RegimeState::TrendUp => dec("0.15"),
        RegimeState::TrendDown => dec("-0.15"),
        _ => Decimal::ZERO,
    };

    clamp_signed_unit(
        clamp_signed_unit(momentum_signal_bps / dec("12")) * dec("0.35")
            + clamp_signed_unit(context.features.trade_flow_imbalance / dec("0.35")) * dec("0.30")
            + clamp_signed_unit(orderbook_signal / dec("0.30")) * dec("0.20")
            + clamp_signed_unit(context.features.vwap_distance_bps / dec("8")) * dec("0.10")
            + regime_bias * dec("0.05"),
    )
}

fn scaled_quote_size(base_quote_size: Decimal, factor: Decimal) -> Decimal {
    if base_quote_size <= Decimal::ZERO || factor <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (base_quote_size * factor).max(dec("0.0001"))
    }
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

fn buy_adverse_selection_penalty_bps(
    context: &StrategyContext,
    toxicity_pressure: Decimal,
    positive_inventory_ratio: Decimal,
    slot_pressure: Decimal,
) -> Decimal {
    let adverse_momentum = normalized_score((-context.features.local_momentum_bps).max(Decimal::ZERO), dec("0.5"), dec("4.0"));
    let adverse_flow =
        normalized_score((-context.features.trade_flow_imbalance).max(Decimal::ZERO), dec("0.15"), dec("0.75"));
    let adverse_book = normalized_score(
        (-orderbook_signal(context)).max(Decimal::ZERO),
        dec("0.10"),
        dec("0.60"),
    );
    adverse_momentum * dec("0.28")
        + adverse_flow * dec("0.34")
        + adverse_book * dec("0.22")
        + toxicity_pressure * dec("0.12")
        + positive_inventory_ratio * dec("0.16")
        + slot_pressure * dec("0.10")
}

fn buy_flow_penalty_bps(context: &StrategyContext) -> Decimal {
    let adverse_flow =
        normalized_score((-context.features.trade_flow_imbalance).max(Decimal::ZERO), dec("0.20"), dec("0.80"));
    let adverse_book = normalized_score(
        (-orderbook_signal(context)).max(Decimal::ZERO),
        dec("0.12"),
        dec("0.65"),
    );
    let adverse_momentum = normalized_score((-context.features.local_momentum_bps).max(Decimal::ZERO), dec("0.5"), dec("5.0"));
    adverse_flow * dec("0.32") + adverse_book * dec("0.18") + adverse_momentum * dec("0.10")
}

fn bid_hazard_score(
    bearish_pressure: Decimal,
    toxicity_pressure: Decimal,
    flow_book_pressure: Decimal,
    vwap_extension: Decimal,
) -> Decimal {
    clamp_unit(
        bearish_pressure * dec("0.55")
            + toxicity_pressure * dec("0.18")
            + flow_book_pressure * dec("0.17")
            + vwap_extension * dec("0.10"),
    )
}

fn ask_hazard_score(
    bullish_pressure: Decimal,
    toxicity_pressure: Decimal,
    flow_book_pressure: Decimal,
    vwap_extension: Decimal,
) -> Decimal {
    clamp_unit(
        bullish_pressure * dec("0.45")
            + toxicity_pressure * dec("0.16")
            + flow_book_pressure * dec("0.14")
            + vwap_extension * dec("0.08"),
    )
}

fn available_order_slots(context: &StrategyContext) -> u32 {
    context
        .max_open_orders_for_symbol
        .saturating_sub(context.open_bot_orders_for_symbol)
}

fn best_candidate_side(candidates: &[CandidateIntent]) -> Side {
    let mut best = &candidates[0];
    for candidate in candidates.iter().skip(1) {
        if candidate.priority_score > best.priority_score {
            best = candidate;
        }
    }
    best.side
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

fn normalized_abs_score(value: Decimal, high: Decimal) -> Decimal {
    if high <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        clamp_unit(value.abs() / high)
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

fn clamp_signed_unit(value: Decimal) -> Decimal {
    if value <= -Decimal::ONE {
        -Decimal::ONE
    } else if value >= Decimal::ONE {
        Decimal::ONE
    } else {
        value
    }
}

fn clamp_signed_bps(value: Decimal, max_abs_bps: Decimal) -> Decimal {
    if max_abs_bps <= Decimal::ZERO {
        Decimal::ZERO
    } else if value <= -max_abs_bps {
        -max_abs_bps
    } else if value >= max_abs_bps {
        max_abs_bps
    } else {
        value
    }
}

fn max_decimal(left: Decimal, right: Decimal) -> Decimal {
    if left >= right { left } else { right }
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
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
                bid_quantity: dec("1.0"),
                ask_price: dec("100.05"),
                ask_quantity: dec("1.0"),
                observed_at: now_utc(),
            }),
            features: FeatureSnapshot {
                symbol: Symbol::BtcUsdc,
                microprice: Some(dec("100.025")),
                top_of_book_depth_quote: dec("5000"),
                orderbook_imbalance: Some(dec("0.02")),
                orderbook_imbalance_rolling: dec("0.01"),
                realized_volatility_bps: dec("3.0"),
                vwap: dec("100.00"),
                vwap_distance_bps: dec("0.10"),
                spread_bps: dec("5.0"),
                spread_mean_bps: dec("4.0"),
                spread_std_bps: dec("0.5"),
                spread_zscore: dec("0.20"),
                trade_flow_imbalance: dec("0.02"),
                trade_flow_rate: dec("0.50"),
                trade_rate_per_sec: dec("1.2"),
                tick_rate_per_sec: dec("2.0"),
                tape_speed: dec("1.0"),
                momentum_1s_bps: dec("0.30"),
                momentum_5s_bps: dec("0.20"),
                momentum_15s_bps: dec("0.10"),
                local_momentum_bps: dec("0.40"),
                liquidity_score: dec("7000"),
                toxicity_score: dec("0.20"),
                volatility_regime: VolatilityRegime::Low,
                computed_at: now_utc(),
            },
            regime: RegimeDecision {
                symbol: Symbol::BtcUsdc,
                state: RegimeState::Range,
                confidence: dec("0.70"),
                reason: "range".to_string(),
                decided_at: now_utc(),
            },
            inventory: InventorySnapshot {
                symbol: Symbol::BtcUsdc,
                base_position: dec("0.002"),
                quote_position: dec("1000"),
                mark_price: Some(dec("100.025")),
                average_entry_price: Some(dec("99.90")),
                position_opened_at: Some(now_utc()),
                last_fill_at: Some(now_utc()),
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

    fn side_quantity(outcome: &StrategyOutcome, side: Side) -> Option<Decimal> {
        outcome
            .intents
            .iter()
            .find(|intent| intent.side == side)
            .map(|intent| intent.quantity)
    }

    #[test]
    fn healthy_range_still_quotes_valid_intents() {
        let outcome = MarketMakingStrategy::default().evaluate(&sample_context());

        assert!(outcome.intents.len() >= 2);
        assert!(side_quantity(&outcome, Side::Buy).unwrap() > Decimal::ZERO);
        assert!(side_quantity(&outcome, Side::Sell).unwrap() > Decimal::ZERO);
        assert!(outcome.intents.iter().all(|intent| intent.edge_after_cost_bps >= dec("1.25")));
        assert!(outcome
            .intents
            .iter()
            .any(|intent| intent.side == Side::Buy && matches!(intent.role, IntentRole::AddRisk)));
        assert!(outcome.standby_reason.is_none());
    }

    #[test]
    fn live_edge_caps_match_reactivation_targets() {
        assert_eq!(required_edge_cap(Symbol::BtcUsdc), dec("1.4"));
        assert_eq!(required_edge_cap(Symbol::EthUsdc), dec("1.6"));
        assert_eq!(probing_edge_threshold(Symbol::BtcUsdc), dec("0.7"));
        assert_eq!(probing_edge_threshold(Symbol::EthUsdc), dec("0.9"));
    }

    #[test]
    fn toxic_context_switches_to_smaller_quote_instead_of_full_block() {
        let healthy = MarketMakingStrategy::default().evaluate(&sample_context());
        let healthy_buy = side_quantity(&healthy, Side::Buy).unwrap();
        let mut context = sample_context();
        context.features.toxicity_score = dec("0.85");

        let outcome = MarketMakingStrategy::default().evaluate(&context);

        let toxic_buy = outcome
            .intents
            .iter()
            .find(|intent| matches!(intent.role, IntentRole::AddRisk) && intent.side == Side::Buy)
            .expect("buy quote should remain available in toxic mode");
        assert!(toxic_buy.quantity < healthy_buy);
    }

    #[test]
    fn probing_is_disabled_under_adverse_momentum_and_flow() {
        let strategy = MarketMakingStrategy::default();
        let mut context = sample_context();
        context.inventory.base_position = Decimal::ZERO;
        context.features.toxicity_score = dec("0.85");
        context.features.spread_bps = dec("5.5");
        context.features.local_momentum_bps = dec("0.8");
        context.features.trade_flow_imbalance = dec("0.18");
        context.features.orderbook_imbalance = Some(dec("0.06"));
        context.features.orderbook_imbalance_rolling = dec("0.04");

        let probe_enabled = strategy.evaluate(&context);
        assert!(probe_enabled
            .intents
            .iter()
            .any(|intent| matches!(intent.role, IntentRole::AddRisk)
                && intent.reason.contains("mm probing bid")));

        context.features.local_momentum_bps = dec("-0.6");
        context.features.trade_flow_imbalance = dec("-0.55");
        context.features.orderbook_imbalance = Some(dec("-0.10"));
        context.features.orderbook_imbalance_rolling = dec("-0.08");

        let probe_disabled = strategy.evaluate(&context);
        assert!(!probe_disabled
            .intents
            .iter()
            .any(|intent| matches!(intent.role, IntentRole::AddRisk)
                && intent.reason.contains("mm probing bid")));
    }

    #[test]
    fn strong_bearish_pressure_keeps_buy_side_small_if_any() {
        let mut context = sample_context();
        context.regime.state = RegimeState::TrendDown;
        context.features.local_momentum_bps = dec("-18");
        context.features.momentum_1s_bps = dec("-12");
        context.features.momentum_5s_bps = dec("-9");
        context.features.trade_flow_imbalance = dec("-0.55");
        context.features.orderbook_imbalance = Some(dec("-0.40"));
        context.features.orderbook_imbalance_rolling = dec("-0.35");
        context.features.vwap_distance_bps = dec("-6.0");
        context.features.spread_bps = dec("6.5");

        let outcome = MarketMakingStrategy::default().evaluate(&context);

        if let Some(buy_quantity) = side_quantity(&outcome, Side::Buy) {
            assert!(buy_quantity < dec("0.0010"));
        }
        assert!(side_quantity(&outcome, Side::Sell).is_some());
    }

    #[test]
    fn strong_bullish_pressure_removes_passive_ask() {
        let mut context = sample_context();
        context.regime.state = RegimeState::TrendUp;
        context.features.local_momentum_bps = dec("18");
        context.features.momentum_1s_bps = dec("12");
        context.features.momentum_5s_bps = dec("9");
        context.features.trade_flow_imbalance = dec("0.55");
        context.features.orderbook_imbalance = Some(dec("0.40"));
        context.features.orderbook_imbalance_rolling = dec("0.35");
        context.features.vwap_distance_bps = dec("6.0");
        context.features.spread_bps = dec("6.5");

        let outcome = MarketMakingStrategy::default().evaluate(&context);

        assert!(!outcome
            .intents
            .iter()
            .any(|intent| intent.side == Side::Sell && !intent.reduce_only));
        assert!(side_quantity(&outcome, Side::Buy).is_some());
    }

    #[test]
    fn inventory_pressure_and_toxicity_reduce_buy_sizing() {
        let strategy = MarketMakingStrategy::default();

        let healthy = strategy.evaluate(&sample_context());
        let healthy_buy = side_quantity(&healthy, Side::Buy).unwrap();

        let mut pressured_context = sample_context();
        pressured_context.inventory.base_position = dec("0.025");
        pressured_context.features.toxicity_score = dec("0.60");
        pressured_context.features.spread_bps = dec("7.0");

        let pressured = strategy.evaluate(&pressured_context);
        let pressured_buy = side_quantity(&pressured, Side::Buy).unwrap_or(Decimal::ZERO);

        assert!(pressured_buy < healthy_buy);
    }

    #[test]
    fn slot_saturation_keeps_only_highest_quality_quote() {
        let mut context = sample_context();
        context.open_bot_orders_for_symbol = 3;
        context.max_open_orders_for_symbol = 4;
        context.inventory.base_position = dec("0.014");

        let outcome = MarketMakingStrategy::default().evaluate(&context);

        assert_eq!(outcome.intents.len(), 1);
    }

    #[test]
    fn stale_inventory_eventually_tightens_then_forces_unwind() {
        let strategy = MarketMakingStrategy::default();
        let mut context = sample_context();
        context.inventory.base_position = dec("0.018");
        context.inventory.average_entry_price = Some(dec("100.02"));
        context.inventory.position_opened_at =
            Some(context.features.computed_at - time::Duration::seconds(50));
        context.features.local_momentum_bps = dec("-5.0");
        context.features.momentum_1s_bps = dec("-4.0");
        context.features.momentum_5s_bps = dec("-3.0");
        context.features.trade_flow_imbalance = dec("-0.18");
        context.features.orderbook_imbalance = Some(dec("-0.12"));
        context.features.orderbook_imbalance_rolling = dec("-0.10");

        let passive_or_tight = strategy.evaluate(&context);
        let exit_intent = passive_or_tight
            .intents
            .iter()
            .find(|intent| intent.reduce_only)
            .expect("reduce-only exit");
        assert!(matches!(
            exit_intent.exit_stage,
            Some(ExitStage::Passive | ExitStage::Tighten)
        ));
        assert!(exit_intent.post_only);

        context.inventory.position_opened_at =
            Some(context.features.computed_at - time::Duration::seconds(90));
        context.features.toxicity_score = dec("0.72");
        context.features.trade_flow_imbalance = dec("-0.40");
        context.features.orderbook_imbalance = Some(dec("-0.28"));
        context.features.orderbook_imbalance_rolling = dec("-0.25");

        let aggressive = strategy.evaluate(&context);
        assert!(
            !aggressive.intents.is_empty(),
            "expected staged exit candidate, got standby={:?}",
            aggressive.standby_reason
        );
        let aggressive_exit = aggressive
            .intents
            .iter()
            .find(|intent| intent.reduce_only)
            .expect("aggressive unwind");
        assert!(matches!(
            aggressive_exit.role,
            IntentRole::ForcedUnwind | IntentRole::EmergencyExit
        ));
        assert!(matches!(
            aggressive_exit.exit_stage,
            Some(ExitStage::Aggressive | ExitStage::Emergency)
        ));
        assert!(!aggressive_exit.post_only);
    }

    #[test]
    fn last_slot_prefers_reduce_risk_exit_over_new_add_risk_quote() {
        let mut context = sample_context();
        context.inventory.base_position = dec("0.020");
        context.inventory.average_entry_price = Some(dec("100.03"));
        context.inventory.position_opened_at =
            Some(context.features.computed_at - time::Duration::seconds(70));
        context.open_bot_orders_for_symbol = 3;
        context.max_open_orders_for_symbol = 4;
        context.features.trade_flow_imbalance = dec("-0.24");
        context.features.orderbook_imbalance = Some(dec("-0.20"));
        context.features.orderbook_imbalance_rolling = dec("-0.18");
        context.features.local_momentum_bps = dec("-3.0");
        context.features.momentum_1s_bps = dec("-2.5");
        context.features.momentum_5s_bps = dec("-2.0");

        let outcome = MarketMakingStrategy::default().evaluate(&context);

        assert_eq!(outcome.intents.len(), 1);
        assert!(outcome.intents[0].reduce_only);
        assert_eq!(outcome.intents[0].side, Side::Sell);
    }

    #[test]
    fn local_min_notional_floor_blocks_tiny_quotes_before_execution() {
        let mut context = sample_context();
        context.best_bid_ask = Some(BestBidAsk {
            symbol: Symbol::BtcUsdc,
            bid_price: dec("60000.00"),
            bid_quantity: dec("2.0"),
            ask_price: dec("60000.01"),
            ask_quantity: dec("2.0"),
            observed_at: now_utc(),
        });
        context.features.microprice = Some(dec("60000.005"));
        context.features.spread_bps = dec("0.0016666667");
        context.soft_inventory_base = dec("0.0010");
        context.max_inventory_base = dec("0.0020");
        context.local_min_notional_quote = dec("15.00");
        context.inventory.base_position = dec("0.0002");

        let strategy = MarketMakingStrategy {
            config: MarketMakingConfig {
                maker_fee_bps: dec("0.75"),
                slippage_buffer_bps: dec("0.15"),
                min_net_edge_bps: dec("0.50"),
                min_market_spread_bps: dec("0.001"),
                max_toxicity_score: dec("0.55"),
                base_skew_bps: dec("10"),
                momentum_skew_weight: dec("0.20"),
                volatility_widening_weight: dec("0.22"),
                quote_size_fraction: dec("0.05"),
                quote_ttl_secs: 6,
                soft_hold_secs: 18,
                stale_hold_secs: 45,
                aggressive_exit_secs: 75,
            },
        };

        let outcome = strategy.evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert!(outcome
            .standby_reason
            .as_deref()
            .unwrap_or_default()
            .contains("deployable"));
    }

    #[test]
    fn tight_spread_major_market_can_still_quote_when_edge_model_is_positive() {
        let mut context = sample_context();
        context.best_bid_ask = Some(BestBidAsk {
            symbol: Symbol::BtcUsdc,
            bid_price: dec("60000.00"),
            bid_quantity: dec("2.0"),
            ask_price: dec("60000.01"),
            ask_quantity: dec("2.0"),
            observed_at: now_utc(),
        });
        context.features.microprice = Some(dec("60020.00"));
        context.features.spread_bps = dec("0.0016666667");
        context.features.realized_volatility_bps = dec("0.15");
        context.features.trade_flow_imbalance = dec("0.10");
        context.features.orderbook_imbalance = Some(dec("0.12"));
        context.features.orderbook_imbalance_rolling = dec("0.10");
        context.features.local_momentum_bps = Decimal::ZERO;
        context.features.momentum_1s_bps = Decimal::ZERO;
        context.features.momentum_5s_bps = Decimal::ZERO;
        context.features.vwap_distance_bps = Decimal::ZERO;
        context.inventory.base_position = dec("0.001");

        let strategy = MarketMakingStrategy {
            config: MarketMakingConfig {
                maker_fee_bps: dec("0.75"),
                slippage_buffer_bps: dec("0.15"),
                min_net_edge_bps: dec("0.50"),
                min_market_spread_bps: dec("0.001"),
                max_toxicity_score: dec("0.55"),
                base_skew_bps: dec("10"),
                momentum_skew_weight: dec("0.20"),
                volatility_widening_weight: dec("0.22"),
                quote_size_fraction: dec("0.10"),
                quote_ttl_secs: 6,
                soft_hold_secs: 18,
                stale_hold_secs: 45,
                aggressive_exit_secs: 75,
            },
        };

        let outcome = strategy.evaluate(&context);

        assert!(!outcome.intents.is_empty());
        assert!(side_quantity(&outcome, Side::Buy).unwrap() > Decimal::ZERO);
    }
}
