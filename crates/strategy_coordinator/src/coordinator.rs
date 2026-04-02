use domain::{RegimeState, RuntimeState, StrategyContext, StrategyOutcome};
use strategy_mm::MarketMakingStrategy;
use strategy_scalp::ScalpingStrategy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategySelection {
    MarketMaking,
    Scalping,
    Standby,
    RiskOff,
}

#[derive(Debug, Clone, Default)]
pub struct DefaultStrategyCoordinator {
    pub market_making: MarketMakingStrategy,
    pub scalping: ScalpingStrategy,
}

impl DefaultStrategyCoordinator {
    pub fn select(&self, context: &StrategyContext) -> StrategySelection {
        if matches!(context.runtime_state, RuntimeState::Paused | RuntimeState::RiskOff | RuntimeState::Shutdown) {
            return StrategySelection::RiskOff;
        }

        match context.regime.state {
            RegimeState::Range => StrategySelection::MarketMaking,
            RegimeState::TrendUp | RegimeState::TrendDown => StrategySelection::Scalping,
            _ => StrategySelection::Standby,
        }
    }

    pub fn evaluate(&self, context: &StrategyContext) -> StrategyOutcome {
        match self.select(context) {
            StrategySelection::MarketMaking => self.market_making.evaluate(context),
            StrategySelection::Scalping => self.scalping.evaluate(context),
            StrategySelection::Standby | StrategySelection::RiskOff => StrategyOutcome {
                intents: Vec::new(),
                standby_reason: Some("coordinator held the symbol in standby".to_string()),
            },
        }
    }
}
