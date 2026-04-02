use common::{Decimal, ids::new_id, now_utc};
use domain::{RegimeState, Side, StrategyContext, StrategyKind, StrategyOutcome, TimeInForce, TradeIntent};

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
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScalpingStrategy {
    pub config: ScalpingConfig,
}

impl ScalpingStrategy {
    pub fn evaluate(&self, context: &StrategyContext) -> StrategyOutcome {
        let Some(best) = &context.best_bid_ask else {
            return standby("scalping waiting for best bid/ask");
        };

        if context.features.toxicity_score > self.config.max_toxicity_score {
            return standby("scalping blocked by toxicity");
        }

        let quote_size = (context.soft_inventory_base * self.config.quote_size_fraction)
            .max(Decimal::from_str_exact("0.0001").unwrap());
        let max_position = context.max_inventory_base * self.config.max_position_fraction;
        let entry_cost_bps = self.config.taker_fee_bps + self.config.slippage_buffer_bps;
        let entry_score = long_entry_score(context);
        let expires_at = Some(now_utc() + time::Duration::seconds(self.config.quote_ttl_secs));

        if context.inventory.base_position > Decimal::ZERO {
            if let Some(exit) = self.exit_intent(context, best.bid_price, quote_size, entry_cost_bps, expires_at) {
                return StrategyOutcome {
                    intents: vec![exit],
                    standby_reason: None,
                };
            }
        }

        if !matches!(context.regime.state, RegimeState::TrendUp | RegimeState::Range) {
            return standby("scalping entry disabled by regime");
        }

        if context.inventory.base_position >= max_position {
            return standby("scalping blocked by position cap");
        }

        if entry_score < self.config.min_entry_score
            || context.features.local_momentum_bps < self.config.min_momentum_bps
        {
            return standby("scalping entry score too weak");
        }

        let expected_edge_bps = context.features.local_momentum_bps.max(context.features.vwap_distance_bps.abs());
        let edge_after_cost_bps = expected_edge_bps - entry_cost_bps;
        if edge_after_cost_bps <= Decimal::ZERO {
            return standby("scalping edge is not positive after costs");
        }

        StrategyOutcome {
            intents: vec![TradeIntent {
                intent_id: format!("scalp-{}", new_id()),
                symbol: context.symbol,
                strategy: StrategyKind::Scalping,
                side: Side::Buy,
                quantity: quote_size.min(max_position - context.inventory.base_position),
                limit_price: Some(best.ask_price),
                max_slippage_bps: self.config.slippage_buffer_bps,
                post_only: false,
                reduce_only: false,
                time_in_force: Some(TimeInForce::Ioc),
                expected_edge_bps,
                expected_fee_bps: self.config.taker_fee_bps,
                expected_slippage_bps: self.config.slippage_buffer_bps,
                edge_after_cost_bps,
                reason: format!(
                    "scalp long entry: score={} mom1={} mom5={} flow={}",
                    entry_score,
                    context.features.momentum_1s_bps,
                    context.features.momentum_5s_bps,
                    context.features.trade_flow_imbalance
                ),
                created_at: now_utc(),
                expires_at,
            }],
            standby_reason: None,
        }
    }

    fn exit_intent(
        &self,
        context: &StrategyContext,
        exit_price: Decimal,
        quote_size: Decimal,
        entry_cost_bps: Decimal,
        expires_at: Option<common::Timestamp>,
    ) -> Option<TradeIntent> {
        let average_entry = context.inventory.average_entry_price?;
        let pnl_bps = if average_entry.is_zero() {
            Decimal::ZERO
        } else {
            ((exit_price - average_entry) / average_entry) * Decimal::from(10_000u32)
        };
        let tp_bps = context.features.realized_volatility_bps.max(Decimal::from(4u32));
        let sl_bps = (context.features.realized_volatility_bps / Decimal::from(2u32)).max(Decimal::from(3u32));
        let momentum_failed = context.features.momentum_1s_bps < Decimal::ZERO
            && context.features.trade_flow_imbalance < Decimal::ZERO;

        if pnl_bps < -sl_bps && !momentum_failed {
            return None;
        }

        if pnl_bps >= tp_bps || pnl_bps <= -sl_bps || momentum_failed {
            let expected_edge_bps = pnl_bps.abs();
            return Some(TradeIntent {
                intent_id: format!("scalp-exit-{}", new_id()),
                symbol: context.symbol,
                strategy: StrategyKind::Scalping,
                side: Side::Sell,
                quantity: quote_size.min(context.inventory.base_position),
                limit_price: Some(exit_price),
                max_slippage_bps: self.config.slippage_buffer_bps,
                post_only: false,
                reduce_only: true,
                time_in_force: Some(TimeInForce::Ioc),
                expected_edge_bps,
                expected_fee_bps: self.config.taker_fee_bps,
                expected_slippage_bps: self.config.slippage_buffer_bps,
                edge_after_cost_bps: expected_edge_bps - entry_cost_bps,
                reason: format!(
                    "scalp exit: pnl_bps={} tp_bps={} sl_bps={} momentum_failed={}",
                    pnl_bps, tp_bps, sl_bps, momentum_failed
                ),
                created_at: now_utc(),
                expires_at,
            });
        }

        None
    }
}

fn long_entry_score(context: &StrategyContext) -> Decimal {
    let momentum = context.features.momentum_1s_bps.max(Decimal::ZERO)
        + context.features.momentum_5s_bps.max(Decimal::ZERO);
    let trade_flow = context.features.trade_flow_imbalance.max(Decimal::ZERO) * Decimal::from(10u32);
    let breakout = context.features.vwap_distance_bps.max(Decimal::ZERO);
    let liquidity = if context.features.liquidity_score > Decimal::ZERO {
        Decimal::from(2u32)
    } else {
        Decimal::ZERO
    };
    (momentum / Decimal::from(2u32)) + trade_flow + breakout + liquidity
}

fn standby(reason: &str) -> StrategyOutcome {
    StrategyOutcome {
        intents: Vec::new(),
        standby_reason: Some(reason.to_string()),
    }
}
