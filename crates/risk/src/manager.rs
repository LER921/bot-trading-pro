use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, now_utc};
use domain::{
    AccountSnapshot, ControlPlaneDecision, HealthState, InventorySnapshot, OpenOrder,
    OperatorCommand, RiskAction, RiskDecision, RiskLimits, RiskMode, RuntimeState, Symbol,
    SystemHealth, TradeIntent,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RiskContext {
    pub account: AccountSnapshot,
    pub health: SystemHealth,
    pub runtime_state: RuntimeState,
    pub inventory: HashMap<Symbol, InventorySnapshot>,
    pub open_orders: Vec<OpenOrder>,
    pub mid_prices: HashMap<Symbol, Decimal>,
}

#[derive(Debug, Clone, Default)]
pub struct GuardHooks {
    pub drawdown_guard_triggered: bool,
    pub reject_rate_guard_triggered: bool,
}

#[async_trait]
pub trait RiskManager: Send + Sync {
    async fn evaluate(&self, intents: Vec<TradeIntent>, context: &RiskContext) -> Result<Vec<RiskDecision>>;

    async fn authorize_operator_command(
        &self,
        command: &OperatorCommand,
        current_state: RuntimeState,
        health: &SystemHealth,
    ) -> Result<ControlPlaneDecision>;
}

#[derive(Debug, Clone)]
pub struct StrictRiskManager {
    pub limits: RiskLimits,
    pub hooks: GuardHooks,
}

impl StrictRiskManager {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            hooks: GuardHooks::default(),
        }
    }

    fn resulting_mode(&self, runtime_state: RuntimeState) -> RiskMode {
        match runtime_state {
            RuntimeState::Reduced => RiskMode::Reduced,
            RuntimeState::Paused => RiskMode::Paused,
            RuntimeState::RiskOff => RiskMode::RiskOff,
            _ => RiskMode::Normal,
        }
    }

    fn is_health_blocking(&self, health: &SystemHealth) -> bool {
        health.overall_state != HealthState::Healthy
    }

    fn limit_for_symbol(&self, symbol: Symbol) -> Option<&domain::SymbolRiskLimits> {
        self.limits
            .per_symbol
            .iter()
            .find(|limits| limits.symbol == symbol)
    }

    fn open_orders_for_symbol(&self, open_orders: &[OpenOrder], symbol: Symbol) -> usize {
        open_orders
            .iter()
            .filter(|order| order.symbol == symbol && order.status.is_open())
            .count()
    }

    fn inventory_for_symbol<'a>(
        &self,
        inventory: &'a HashMap<Symbol, InventorySnapshot>,
        symbol: Symbol,
    ) -> Option<&'a InventorySnapshot> {
        inventory.get(&symbol)
    }

    fn notional_for_intent(&self, intent: &TradeIntent, context: &RiskContext) -> Decimal {
        let reference_price = intent
            .limit_price
            .or_else(|| context.mid_prices.get(&intent.symbol).copied())
            .unwrap_or(Decimal::ZERO);
        intent.quantity * reference_price
    }

    fn would_reduce_inventory(&self, position: Decimal, intent: &TradeIntent) -> bool {
        (position > Decimal::ZERO && matches!(intent.side, domain::Side::Sell))
            || (position < Decimal::ZERO && matches!(intent.side, domain::Side::Buy))
    }

    fn apply_reduce_only_if_needed(
        &self,
        intent: &TradeIntent,
        position: Decimal,
        max_inventory_base: Decimal,
    ) -> Option<TradeIntent> {
        if max_inventory_base.is_zero() {
            return Some(intent.clone());
        }

        let trigger = max_inventory_base * self.limits.inventory.reduce_only_trigger_ratio;
        if position.abs() < trigger {
            return Some(intent.clone());
        }

        if !self.would_reduce_inventory(position, intent) {
            return None;
        }

        let mut reduced = intent.clone();
        reduced.reduce_only = true;
        Some(reduced)
    }

    fn check_drawdown_guard(&self) -> Option<String> {
        self.hooks
            .drawdown_guard_triggered
            .then(|| "drawdown guard hook triggered".to_string())
    }

    fn check_reject_rate_guard(&self) -> Option<String> {
        self.hooks
            .reject_rate_guard_triggered
            .then(|| "reject-rate guard hook triggered".to_string())
    }
}

#[async_trait]
impl RiskManager for StrictRiskManager {
    async fn evaluate(&self, intents: Vec<TradeIntent>, context: &RiskContext) -> Result<Vec<RiskDecision>> {
        let resulting_mode = self.resulting_mode(context.runtime_state);
        let runtime_block = !context.runtime_state.allows_new_orders();
        let health_block = self.is_health_blocking(&context.health);
        let drawdown_guard = self.check_drawdown_guard();
        let reject_rate_guard = self.check_reject_rate_guard();
        let mut projected_positions: HashMap<Symbol, Decimal> = context
            .inventory
            .iter()
            .map(|(symbol, inventory)| (*symbol, inventory.base_position))
            .collect();
        let mut projected_open_orders: HashMap<Symbol, usize> = self
            .limits
            .per_symbol
            .iter()
            .map(|limits| {
                (
                    limits.symbol,
                    self.open_orders_for_symbol(&context.open_orders, limits.symbol),
                )
            })
            .collect();

        let mut decisions = Vec::with_capacity(intents.len());
        for intent in intents {
            let Some(symbol_limits) = self.limit_for_symbol(intent.symbol) else {
                decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason: "missing per-symbol risk limits".to_string(),
                    resulting_mode,
                    decided_at: now_utc(),
                });
                continue;
            };

            if runtime_block {
                decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason: "runtime state does not allow new orders".to_string(),
                    resulting_mode,
                    decided_at: now_utc(),
                });
                continue;
            }

            if health_block {
                decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason: format!("health is not healthy: {:?}", context.health.overall_state),
                    resulting_mode,
                    decided_at: now_utc(),
                });
                continue;
            }

            if let Some(reason) = drawdown_guard.clone().or(reject_rate_guard.clone()) {
                decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason,
                    resulting_mode: RiskMode::RiskOff,
                    decided_at: now_utc(),
                });
                continue;
            }

            let current_position = projected_positions
                .get(&intent.symbol)
                .copied()
                .or_else(|| {
                    self.inventory_for_symbol(&context.inventory, intent.symbol)
                        .map(|inventory| inventory.base_position)
                })
                .unwrap_or(Decimal::ZERO);

            if projected_open_orders
                .get(&intent.symbol)
                .copied()
                .unwrap_or_default()
                >= symbol_limits.max_open_orders as usize
            {
                decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason: "max open orders reached for symbol".to_string(),
                    resulting_mode,
                    decided_at: now_utc(),
                });
                continue;
            }

            if self.notional_for_intent(&intent, context) > symbol_limits.max_quote_notional {
                decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason: "intent notional exceeds per-symbol limit".to_string(),
                    resulting_mode,
                    decided_at: now_utc(),
                });
                continue;
            }

            let projected_after = project_inventory_position(current_position, &intent);
            if projected_after.abs() > symbol_limits.max_inventory_base
                && !self.would_reduce_inventory(current_position, &intent)
            {
                decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason: "intent would breach max inventory".to_string(),
                    resulting_mode,
                    decided_at: now_utc(),
                });
                continue;
            }

            match self.apply_reduce_only_if_needed(
                &intent,
                current_position,
                symbol_limits.max_inventory_base,
            ) {
                Some(approved_intent) => {
                    let action = if approved_intent.reduce_only && !intent.reduce_only {
                        RiskAction::ReduceOnly
                    } else {
                        RiskAction::Approve
                    };

                    projected_positions
                        .insert(intent.symbol, project_inventory_position(current_position, &approved_intent));
                    *projected_open_orders.entry(intent.symbol).or_default() += 1;

                    decisions.push(RiskDecision {
                        action,
                        original_intent: intent,
                        approved_intent: Some(approved_intent),
                        reason: if matches!(action, RiskAction::ReduceOnly) {
                            "inventory imbalance forces reduce-only".to_string()
                        } else {
                            "intent approved by strict risk manager".to_string()
                        },
                        resulting_mode,
                        decided_at: now_utc(),
                    });
                }
                None => decisions.push(RiskDecision {
                    action: RiskAction::Block,
                    original_intent: intent,
                    approved_intent: None,
                    reason: "inventory imbalance allows only reduce-only orders".to_string(),
                    resulting_mode: RiskMode::ReduceOnly,
                    decided_at: now_utc(),
                }),
            }
        }

        Ok(decisions)
    }

    async fn authorize_operator_command(
        &self,
        command: &OperatorCommand,
        current_state: RuntimeState,
        health: &SystemHealth,
    ) -> Result<ControlPlaneDecision> {
        let accepted = match command {
            OperatorCommand::Resume => health.overall_state == HealthState::Healthy,
            _ => true,
        };

        Ok(ControlPlaneDecision {
            accepted,
            message: if accepted {
                "operator command accepted by risk manager".to_string()
            } else {
                "operator command rejected because health is not healthy".to_string()
            },
            resulting_state: if accepted {
                command.target_state(current_state)
            } else {
                current_state
            },
            decided_at: now_utc(),
        })
    }
}

fn project_inventory_position(position: Decimal, intent: &TradeIntent) -> Decimal {
    match intent.side {
        domain::Side::Buy => position + intent.quantity,
        domain::Side::Sell => position - intent.quantity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{
        Balance, RuntimeState, StrategyKind, Symbol, SystemHealth,
    };

    fn sample_limits() -> RiskLimits {
        RiskLimits {
            per_symbol: vec![domain::SymbolRiskLimits {
                symbol: Symbol::BtcUsdc,
                max_inventory_base: Decimal::from_str_exact("0.10").unwrap(),
                soft_inventory_base: Decimal::from_str_exact("0.05").unwrap(),
                max_quote_notional: Decimal::from(1_000u32),
                max_open_orders: 2,
            }],
            inventory: domain::InventoryControl {
                quote_skew_bps_per_inventory_unit: Decimal::ONE,
                neutralization_clip_fraction: Decimal::from_str_exact("0.20").unwrap(),
                reduce_only_trigger_ratio: Decimal::from_str_exact("0.90").unwrap(),
            },
            max_daily_loss_usdc: Decimal::from(100u32),
            max_symbol_drawdown_usdc: Decimal::from(50u32),
            stale_market_data_ms: 1_000,
            stale_account_events_ms: 1_000,
            max_clock_drift_ms: 100,
        }
    }

    fn sample_context() -> RiskContext {
        let now = now_utc();
        RiskContext {
            account: AccountSnapshot {
                balances: vec![Balance {
                    asset: "USDC".to_string(),
                    free: Decimal::from(1_000u32),
                    locked: Decimal::ZERO,
                }],
                updated_at: now,
            },
            health: SystemHealth::healthy(now),
            runtime_state: RuntimeState::Trading,
            inventory: HashMap::new(),
            open_orders: Vec::new(),
            mid_prices: HashMap::from([(Symbol::BtcUsdc, Decimal::from(60_000u32))]),
        }
    }

    fn sample_intent() -> TradeIntent {
        TradeIntent {
            intent_id: "intent-1".to_string(),
            symbol: Symbol::BtcUsdc,
            strategy: StrategyKind::MarketMaking,
            side: domain::Side::Buy,
            quantity: Decimal::from_str_exact("0.001").unwrap(),
            limit_price: Some(Decimal::from(60_000u32)),
            max_slippage_bps: Decimal::from(2u32),
            post_only: true,
            reduce_only: false,
            time_in_force: Some(domain::TimeInForce::Gtc),
            reason: "test".to_string(),
            created_at: now_utc(),
        }
    }

    #[tokio::test]
    async fn blocks_when_runtime_disallows_orders() {
        let manager = StrictRiskManager::new(sample_limits());
        let mut context = sample_context();
        context.runtime_state = RuntimeState::Paused;

        let decisions = manager.evaluate(vec![sample_intent()], &context).await.unwrap();
        assert_eq!(decisions[0].action, RiskAction::Block);
    }

    #[tokio::test]
    async fn blocks_when_health_is_degraded() {
        let manager = StrictRiskManager::new(sample_limits());
        let mut context = sample_context();
        context.health.overall_state = HealthState::Degraded;

        let decisions = manager.evaluate(vec![sample_intent()], &context).await.unwrap();
        assert_eq!(decisions[0].action, RiskAction::Block);
    }

    #[tokio::test]
    async fn forces_reduce_only_when_inventory_is_excessive() {
        let manager = StrictRiskManager::new(sample_limits());
        let mut context = sample_context();
        context.inventory.insert(
            Symbol::BtcUsdc,
            InventorySnapshot {
                symbol: Symbol::BtcUsdc,
                base_position: Decimal::from_str_exact("0.095").unwrap(),
                quote_position: Decimal::ZERO,
                mark_price: Some(Decimal::from(60_000u32)),
                updated_at: now_utc(),
            },
        );

        let mut intent = sample_intent();
        intent.side = domain::Side::Sell;

        let decisions = manager.evaluate(vec![intent], &context).await.unwrap();
        assert_eq!(decisions[0].action, RiskAction::ReduceOnly);
        assert!(decisions[0]
            .approved_intent
            .as_ref()
            .expect("approved intent")
            .reduce_only);
    }
}
