use common::{Decimal, ids::new_id, now_utc};
use domain::{RegimeState, Side, StrategyContext, StrategyKind, StrategyOutcome, TimeInForce, TradeIntent};

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
        if !matches!(context.regime.state, RegimeState::Range | RegimeState::TrendUp | RegimeState::TrendDown) {
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
        let inventory_ratio = if context.max_inventory_base.is_zero() {
            Decimal::ZERO
        } else {
            context.inventory.base_position / context.max_inventory_base
        };
        let inventory_skew_bps = -inventory_ratio * self.config.base_skew_bps;
        let momentum_skew_bps = context.features.local_momentum_bps * self.config.momentum_skew_weight;
        let volatility_widening_bps =
            context.features.realized_volatility_bps * self.config.volatility_widening_weight;
        let total_cost_bps =
            self.config.maker_fee_bps + self.config.slippage_buffer_bps + self.config.min_net_edge_bps;
        let dynamic_half_spread_bps = (context.features.spread_bps / Decimal::from(2u32))
            .max(total_cost_bps + volatility_widening_bps);

        let quote_size = (context.soft_inventory_base * self.config.quote_size_fraction)
            .max(Decimal::from_str_exact("0.0001").unwrap());
        let reduce_only_trigger = context.max_inventory_base * Decimal::from_str_exact("0.90").unwrap();
        let quote_center = shift_price_bps(fair, inventory_skew_bps + momentum_skew_bps);
        let buy_price = shift_price_bps(quote_center, -dynamic_half_spread_bps);
        let sell_price = shift_price_bps(quote_center, dynamic_half_spread_bps);
        let edge_after_cost_bps = dynamic_half_spread_bps - total_cost_bps;

        if edge_after_cost_bps <= Decimal::ZERO {
            return standby("net edge is not positive after maker costs");
        }

        let mut intents = Vec::new();
        let expires_at = Some(now_utc() + time::Duration::seconds(self.config.quote_ttl_secs));

        if context.inventory.base_position <= reduce_only_trigger {
            intents.push(build_intent(
                context,
                Side::Buy,
                quote_size,
                buy_price,
                false,
                dynamic_half_spread_bps,
                total_cost_bps,
                edge_after_cost_bps,
                format!(
                    "mm bid: fair={} inv_skew_bps={} mom_skew_bps={} vol_widen_bps={}",
                    fair, inventory_skew_bps, momentum_skew_bps, volatility_widening_bps
                ),
                expires_at,
            ));
        }

        if context.inventory.base_position > Decimal::ZERO {
            intents.push(build_intent(
                context,
                Side::Sell,
                quote_size.min(context.inventory.base_position),
                sell_price,
                context.inventory.base_position.abs() >= reduce_only_trigger,
                dynamic_half_spread_bps,
                total_cost_bps,
                edge_after_cost_bps,
                format!(
                    "mm ask: fair={} inv_skew_bps={} mom_skew_bps={} vol_widen_bps={}",
                    fair, inventory_skew_bps, momentum_skew_bps, volatility_widening_bps
                ),
                expires_at,
            ));
        }

        if intents.is_empty() {
            standby("inventory imbalance allows only passive unwind")
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
    expected_cost_bps: Decimal,
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
        expected_fee_bps: expected_cost_bps,
        expected_slippage_bps: Decimal::ZERO,
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
        price * (Decimal::ONE + bps / Decimal::from(10_000u32))
    }
}
