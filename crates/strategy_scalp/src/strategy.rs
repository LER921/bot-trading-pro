use common::{ids::new_id, Decimal, now_utc};
use domain::{RegimeState, Side, StrategyContext, StrategyKind, StrategyOutcome, TimeInForce, TradeIntent};

#[derive(Debug, Clone)]
pub struct ScalpingConfig {
    pub min_net_edge_bps: Decimal,
    pub max_holding_seconds: u64,
}

impl Default for ScalpingConfig {
    fn default() -> Self {
        Self {
            min_net_edge_bps: Decimal::from(3u32),
            max_holding_seconds: 15,
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
            return StrategyOutcome {
                intents: Vec::new(),
                standby_reason: Some("scalping waiting for best bid/ask".to_string()),
            };
        };

        let size = (context.soft_inventory_base / Decimal::from(8u32)).max(Decimal::ZERO);
        if size.is_zero() {
            return StrategyOutcome {
                intents: Vec::new(),
                standby_reason: Some("scalping disabled because size is zero".to_string()),
            };
        }

        let outcome = match context.regime.state {
            RegimeState::TrendUp if context.inventory.base_position < context.soft_inventory_base => {
                Some(build_intent(
                    context,
                    Side::Buy,
                    size,
                    Some(best.ask_price),
                    false,
                    "simple scalp follow-up buy",
                ))
            }
            RegimeState::TrendDown if context.inventory.base_position > Decimal::ZERO => Some(build_intent(
                context,
                Side::Sell,
                size.min(context.inventory.base_position),
                Some(best.bid_price),
                true,
                "simple scalp de-risk sell",
            )),
            _ => None,
        };

        match outcome {
            Some(intent) => StrategyOutcome {
                intents: vec![intent],
                standby_reason: None,
            },
            None => StrategyOutcome {
                intents: Vec::new(),
                standby_reason: Some("scalping disabled by regime or inventory filter".to_string()),
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
    reason: &str,
) -> TradeIntent {
    TradeIntent {
        intent_id: format!("scalp-{}", new_id()),
        symbol: context.symbol,
        strategy: StrategyKind::Scalping,
        side,
        quantity,
        limit_price,
        max_slippage_bps: Decimal::from(6u32),
        post_only: false,
        reduce_only,
        time_in_force: Some(TimeInForce::Ioc),
        reason: reason.to_string(),
        created_at: now_utc(),
    }
}
