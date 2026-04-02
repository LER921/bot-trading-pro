use common::{Decimal, ids::new_id, now_utc};
use domain::{
    RegimeState, Side, StrategyContext, StrategyKind, StrategyOutcome, TimeInForce, TradeIntent,
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
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketMakingStrategy {
    pub config: MarketMakingConfig,
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

        if context.features.spread_bps < self.config.min_market_spread_bps {
            return standby("spread too tight after costs");
        }

        if context.features.toxicity_score > self.config.max_toxicity_score {
            return standby("book too toxic for market making");
        }

        if context.features.liquidity_score <= Decimal::ZERO
            || context.features.top_of_book_depth_quote <= Decimal::ZERO
        {
            return standby("liquidity too weak for market making");
        }

        if context.inventory.base_position.abs() >= context.max_inventory_base {
            return standby("inventory ceiling reached");
        }

        let mid = (best.bid_price + best.ask_price) / Decimal::from(2u32);
        let fair = context.features.microprice.unwrap_or(mid);
        let inventory_ratio = inventory_ratio(context);
        let positive_inventory_ratio = clamp_unit(inventory_ratio.max(Decimal::ZERO));
        let orderbook_signal = orderbook_signal(context);
        let momentum_signal_bps = short_momentum_signal_bps(context);
        let directional_pressure = directional_pressure_score(context, momentum_signal_bps, orderbook_signal);
        let toxicity_pressure =
            normalized_score(context.features.toxicity_score, dec("0.25"), self.config.max_toxicity_score);
        let flow_book_pressure = max_decimal(
            normalized_abs_score(context.features.trade_flow_imbalance, dec("0.45")),
            normalized_abs_score(orderbook_signal, dec("0.35")),
        );
        let vwap_extension = normalized_abs_score(context.features.vwap_distance_bps, dec("8"));
        let momentum_extension = normalized_abs_score(momentum_signal_bps, dec("12"));

        let inventory_skew_bps = -inventory_ratio * self.config.base_skew_bps;
        let directional_center_skew_bps = directional_pressure * dec("6");
        let mean_reversion_skew_bps =
            clamp_signed_bps(-context.features.vwap_distance_bps * dec("0.30"), dec("4"));
        let quote_center = shift_price_bps(
            fair,
            inventory_skew_bps + directional_center_skew_bps + mean_reversion_skew_bps,
        );

        let minimum_required_edge_bps =
            self.config.maker_fee_bps + self.config.slippage_buffer_bps + self.config.min_net_edge_bps;
        let volatility_widening_bps =
            context.features.realized_volatility_bps * self.config.volatility_widening_weight;
        let toxicity_widening_bps = toxicity_pressure * dec("5");
        let directional_widening_bps = directional_pressure.abs() * dec("4");
        let imbalance_widening_bps = flow_book_pressure * dec("3");
        let extension_widening_bps = (vwap_extension * dec("1.5")) + (momentum_extension * dec("1.0"));
        let dynamic_half_spread_bps = (context.features.spread_bps / Decimal::from(2u32)).max(
            minimum_required_edge_bps
                + volatility_widening_bps
                + toxicity_widening_bps
                + directional_widening_bps
                + imbalance_widening_bps
                + extension_widening_bps,
        );

        let market_half_spread_bps = context.features.spread_bps / Decimal::from(2u32);
        if dynamic_half_spread_bps >= market_half_spread_bps * dec("4.0") {
            return standby("required passive edge exceeds available market spread");
        }

        let reduce_only_trigger = context.max_inventory_base * dec("0.90");
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
        let bid_hazard = clamp_unit(
            bearish_pressure * dec("0.65")
                + toxicity_pressure * dec("0.20")
                + flow_book_pressure * dec("0.10")
                + vwap_extension * dec("0.05"),
        );
        let ask_hazard = clamp_unit(
            bullish_pressure * dec("0.65")
                + toxicity_pressure * dec("0.20")
                + flow_book_pressure * dec("0.10")
                + vwap_extension * dec("0.05"),
        );

        let regime_size_factor = match context.regime.state {
            RegimeState::Range => Decimal::ONE,
            RegimeState::TrendUp | RegimeState::TrendDown => dec("0.70"),
            _ => Decimal::ZERO,
        };
        let toxicity_size_factor = (Decimal::ONE - toxicity_pressure * dec("0.60")).max(dec("0.35"));
        let base_quote_size = context.soft_inventory_base * self.config.quote_size_fraction;
        let bid_edge_size_factor = clamp_unit(bid_edge_after_cost_bps.max(Decimal::ZERO) / dec("6")).max(dec("0.25"));
        let ask_edge_size_factor = clamp_unit(ask_edge_after_cost_bps.max(Decimal::ZERO) / dec("6")).max(dec("0.25"));
        let bid_inventory_size_factor =
            (Decimal::ONE - positive_inventory_ratio * dec("0.80")).max(dec("0.20"));
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

        let bid_allowed = context.inventory.base_position <= reduce_only_trigger
            && bid_hazard < dec("0.72")
            && bid_edge_after_cost_bps >= self.config.min_net_edge_bps
            && bid_size > Decimal::ZERO;
        let ask_allowed = context.inventory.base_position > Decimal::ZERO
            && ask_hazard < dec("0.72")
            && ask_edge_after_cost_bps >= self.config.min_net_edge_bps
            && ask_size > Decimal::ZERO;

        let expires_at = Some(now_utc() + time::Duration::seconds(self.config.quote_ttl_secs));
        let mut intents = Vec::new();
        let mut withheld = Vec::new();

        if bid_allowed {
            intents.push(build_intent(
                context,
                Side::Buy,
                bid_size,
                buy_price,
                false,
                bid_expected_edge_bps,
                self.config.maker_fee_bps,
                self.config.slippage_buffer_bps,
                bid_edge_after_cost_bps,
                format!(
                    "mm bid: fair={} inv_skew_bps={} pressure_skew_bps={} mean_rev_skew_bps={} half_spread_bps={} bid_hazard={} dir_pressure={} tox={} size={}",
                    fair,
                    inventory_skew_bps,
                    directional_center_skew_bps,
                    mean_reversion_skew_bps,
                    dynamic_half_spread_bps,
                    bid_hazard,
                    directional_pressure,
                    context.features.toxicity_score,
                    bid_size,
                ),
                expires_at,
            ));
        } else {
            withheld.push(format!(
                "bid gated: edge_after_cost_bps={} hazard={} inv_ratio={}",
                bid_edge_after_cost_bps, bid_hazard, inventory_ratio
            ));
        }

        if ask_allowed {
            intents.push(build_intent(
                context,
                Side::Sell,
                ask_size.min(context.inventory.base_position),
                sell_price,
                context.inventory.base_position.abs() >= reduce_only_trigger,
                ask_expected_edge_bps,
                self.config.maker_fee_bps,
                self.config.slippage_buffer_bps,
                ask_edge_after_cost_bps,
                format!(
                    "mm ask: fair={} inv_skew_bps={} pressure_skew_bps={} mean_rev_skew_bps={} half_spread_bps={} ask_hazard={} dir_pressure={} tox={} size={}",
                    fair,
                    inventory_skew_bps,
                    directional_center_skew_bps,
                    mean_reversion_skew_bps,
                    dynamic_half_spread_bps,
                    ask_hazard,
                    directional_pressure,
                    context.features.toxicity_score,
                    ask_size.min(context.inventory.base_position),
                ),
                expires_at,
            ));
        } else if context.inventory.base_position > Decimal::ZERO {
            withheld.push(format!(
                "ask gated: edge_after_cost_bps={} hazard={} inv_ratio={}",
                ask_edge_after_cost_bps, ask_hazard, inventory_ratio
            ));
        }

        if intents.is_empty() {
            standby(&format!("market making withheld: {}", withheld.join("; ")))
        } else {
            StrategyOutcome {
                intents,
                standby_reason: None,
            }
        }
    }
}

fn build_intent(
    context: &StrategyContext,
    side: Side,
    quantity: Decimal,
    limit_price: Decimal,
    reduce_only: bool,
    expected_edge_bps: Decimal,
    expected_fee_bps: Decimal,
    expected_slippage_bps: Decimal,
    edge_after_cost_bps: Decimal,
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
        post_only: true,
        reduce_only,
        time_in_force: Some(TimeInForce::Gtc),
        expected_edge_bps,
        expected_fee_bps,
        expected_slippage_bps,
        edge_after_cost_bps,
        reason,
        created_at: now_utc(),
        expires_at,
    }
}

fn standby(reason: &str) -> StrategyOutcome {
    StrategyOutcome {
        intents: Vec::new(),
        standby_reason: Some(reason.to_string()),
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
                base_position: dec("0.010"),
                quote_position: dec("1000"),
                mark_price: Some(dec("100.025")),
                average_entry_price: Some(dec("99.90")),
                updated_at: now_utc(),
            },
            soft_inventory_base: dec("0.020"),
            max_inventory_base: dec("0.050"),
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

        assert_eq!(outcome.intents.len(), 2);
        assert!(side_quantity(&outcome, Side::Buy).unwrap() > Decimal::ZERO);
        assert!(side_quantity(&outcome, Side::Sell).unwrap() > Decimal::ZERO);
        assert!(outcome.intents.iter().all(|intent| intent.edge_after_cost_bps >= dec("1.0")));
        assert!(outcome.standby_reason.is_none());
    }

    #[test]
    fn toxic_context_blocks_market_making() {
        let mut context = sample_context();
        context.features.toxicity_score = dec("0.85");

        let outcome = MarketMakingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert_eq!(
            outcome.standby_reason.as_deref(),
            Some("book too toxic for market making")
        );
    }

    #[test]
    fn strong_bearish_pressure_removes_passive_bid() {
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

        assert!(side_quantity(&outcome, Side::Buy).is_none());
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

        assert!(side_quantity(&outcome, Side::Sell).is_none());
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
        let pressured_buy = side_quantity(&pressured, Side::Buy).unwrap();

        assert!(pressured_buy < healthy_buy);
    }

    #[test]
    fn insufficient_edge_after_costs_produces_no_quotes() {
        let mut context = sample_context();
        context.features.spread_bps = dec("1.0");
        context.features.realized_volatility_bps = Decimal::ZERO;
        context.features.local_momentum_bps = Decimal::ZERO;
        context.features.momentum_1s_bps = Decimal::ZERO;
        context.features.momentum_5s_bps = Decimal::ZERO;
        context.features.trade_flow_imbalance = Decimal::ZERO;
        context.features.orderbook_imbalance = Some(Decimal::ZERO);
        context.features.orderbook_imbalance_rolling = Decimal::ZERO;
        context.features.vwap_distance_bps = Decimal::ZERO;

        let outcome = MarketMakingStrategy::default().evaluate(&context);

        assert!(outcome.intents.is_empty());
        assert!(outcome
            .standby_reason
            .as_deref()
            .unwrap_or_default()
            .contains("edge"));
    }
}
