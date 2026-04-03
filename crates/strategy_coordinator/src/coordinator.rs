use common::Decimal;
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

#[derive(Debug, Clone)]
pub struct StrategySelectionConfig {
    pub scalp_activation_momentum_bps: Decimal,
    pub scalp_activation_flow_imbalance: Decimal,
}

impl Default for StrategySelectionConfig {
    fn default() -> Self {
        Self {
            scalp_activation_momentum_bps: Decimal::from(8u32),
            scalp_activation_flow_imbalance: Decimal::from_str_exact("0.20").unwrap(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DefaultStrategyCoordinator {
    pub selection: StrategySelectionConfig,
    pub market_making: MarketMakingStrategy,
    pub scalping: ScalpingStrategy,
}

impl DefaultStrategyCoordinator {
    pub fn new(
        selection: StrategySelectionConfig,
        market_making: MarketMakingStrategy,
        scalping: ScalpingStrategy,
    ) -> Self {
        Self {
            selection,
            market_making,
            scalping,
        }
    }

    pub fn select(&self, context: &StrategyContext) -> StrategySelection {
        if matches!(context.runtime_state, RuntimeState::Paused | RuntimeState::RiskOff | RuntimeState::Shutdown) {
            return StrategySelection::RiskOff;
        }

        match context.regime.state {
            RegimeState::Range => StrategySelection::MarketMaking,
            RegimeState::TrendUp | RegimeState::TrendDown => {
                if context.features.local_momentum_bps.abs()
                    >= self.selection.scalp_activation_momentum_bps
                    && context.features.trade_flow_imbalance.abs()
                        >= self.selection.scalp_activation_flow_imbalance
                {
                    StrategySelection::Scalping
                } else {
                    StrategySelection::MarketMaking
                }
            }
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
                entry_block_reason: None,
                best_expected_realized_edge_bps: None,
                adverse_selection_hits: 0,
            },
        }
    }
}
