use common::{ids::new_id, Decimal, now_utc};
use domain::{RegimeState, Side, StrategyContext, StrategyKind, StrategyOutcome, TimeInForce, TradeIntent};

#[derive(Debug, Clone)]
pub struct MarketMakingConfig {
    pub min_edge_bps: Decimal,
    pub quote_size_base: Decimal,
    pub max_layers: u8,
}

impl Default for MarketMakingConfig {
    fn default() -> Self {
        Self {
            min_edge_bps: Decimal::from(2u32),
            quote_size_base: Decimal::ZERO,
            max_layers: 1,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketMakingStrategy {
    pub config: MarketMakingConfig,
}

impl MarketMakingStrategy {
    pub fn evaluate(&self, context: &StrategyContext) -> StrategyOutcome {
        match context.regime.state {
            RegimeState::Range | RegimeState::TrendUp | RegimeState::TrendDown => {
                let Some(best) = &context.best_bid_ask else {
                    return StrategyOutcome {
                        intents: Vec::new(),
                        standby_reason: Some("market making waiting for best bid/ask".to_string()),
                    };
                };

                let inventory_abs = context.inventory.base_position.abs();
                let size = if self.config.quote_size_base.is_zero() {
                    (context.soft_inventory_base / Decimal::from(10u32)).max(Decimal::ZERO)
                } else {
                    self.config.quote_size_base
                };

                if size.is_zero() || inventory_abs >= context.max_inventory_base {
                    return StrategyOutcome {
                        intents: Vec::new(),
                        standby_reason: Some("market making blocked by inventory ceiling".to_string()),
                    };
                }

                let bid_price = best.bid_price;
                let ask_price = best.ask_price;
                let mut intents = Vec::new();

                if context.inventory.base_position <= context.soft_inventory_base {
                    intents.push(build_intent(context, Side::Buy, size, Some(bid_price), false));
                }

                if context.inventory.base_position >= -context.soft_inventory_base {
                    intents.push(build_intent(context, Side::Sell, size, Some(ask_price), false));
                }

                StrategyOutcome {
                    intents,
                    standby_reason: None,
                }
            }
            _ => StrategyOutcome {
                intents: Vec::new(),
                standby_reason: Some("market making disabled by regime".to_string()),
            },
        }
    }
}

fn build_intent(
    context: &StrategyContext,
    side: Side,
    quantity: Decimal,
    limit_price: Option<Decimal>,
    reduce_only: bool,
) -> TradeIntent {
    TradeIntent {
        intent_id: format!("mm-{}", new_id()),
        symbol: context.symbol,
        strategy: StrategyKind::MarketMaking,
        side,
        quantity,
        limit_price,
        max_slippage_bps: Decimal::from(4u32),
        post_only: true,
        reduce_only,
        time_in_force: Some(TimeInForce::Gtc),
        reason: "simple market making intent".to_string(),
        created_at: now_utc(),
    }
}
