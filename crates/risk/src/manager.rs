use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, now_utc};
use domain::{
    AccountSnapshot, ControlPlaneDecision, ExecutionStats, HealthState, InventorySnapshot, OpenOrder,
    OperatorCommand, PnlSnapshot, RiskAction, RiskDecision, RiskLimits, RiskMode, RuntimeState,
    Symbol, SystemHealth, TradeIntent,
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
    pub pnl: PnlSnapshot,
    pub execution_stats: ExecutionStats,
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

    fn mark_price_for_symbol(&self, context: &RiskContext, symbol: Symbol) -> Decimal {
        context
            .mid_prices
            .get(&symbol)
            .copied()
            .or_else(|| {
                context
                    .inventory
                    .get(&symbol)
                    .and_then(|inventory| inventory.mark_price)
            })
            .unwrap_or(Decimal::ZERO)
    }

    fn projected_symbol_exposure_quote(
        &self,
        inventory: &HashMap<Symbol, InventorySnapshot>,
        projected_open_buys: &HashMap<Symbol, Decimal>,
        projected_open_sells: &HashMap<Symbol, Decimal>,
        context: &RiskContext,
        symbol: Symbol,
    ) -> Decimal {
        worst_case_symbol_inventory(
            inventory
                .get(&symbol)
                .map(|snapshot| snapshot.base_position)
                .unwrap_or(Decimal::ZERO),
            projected_open_buys
                .get(&symbol)
                .copied()
                .unwrap_or(Decimal::ZERO),
            projected_open_sells
                .get(&symbol)
                .copied()
                .unwrap_or(Decimal::ZERO),
        ) * self.mark_price_for_symbol(context, symbol)
    }

    fn total_projected_exposure_quote(
        &self,
        inventory: &HashMap<Symbol, InventorySnapshot>,
        projected_open_buys: &HashMap<Symbol, Decimal>,
        projected_open_sells: &HashMap<Symbol, Decimal>,
        context: &RiskContext,
    ) -> Decimal {
        self.limits.per_symbol.iter().fold(Decimal::ZERO, |acc, limits| {
            acc + self.projected_symbol_exposure_quote(
                inventory,
                projected_open_buys,
                projected_open_sells,
                context,
                limits.symbol,
            )
        })
    }

    fn check_drawdown_guard(&self, context: &RiskContext) -> Option<String> {
        if self.hooks.drawdown_guard_triggered {
            return Some("drawdown guard hook triggered".to_string());
        }

        if context.pnl.daily_pnl_quote <= -self.limits.max_daily_loss_usdc {
            return Some("daily loss limit breached".to_string());
        }

        if context.pnl.drawdown_quote >= self.limits.max_daily_loss_usdc {
            return Some("global drawdown limit breached".to_string());
        }

        if context
            .pnl
            .per_symbol
            .iter()
            .any(|symbol| symbol.drawdown_quote >= self.limits.max_symbol_drawdown_usdc)
        {
            return Some("per-symbol drawdown limit breached".to_string());
        }

        None
    }

    fn check_reject_rate_guard(&self, context: &RiskContext) -> Option<String> {
        if self.hooks.reject_rate_guard_triggered {
            return Some("reject-rate guard hook triggered".to_string());
        }

        if context.execution_stats.total_submitted >= 10
            && context.execution_stats.risk_scored_rejections >= 3
            && context.execution_stats.risk_reject_rate >= self.limits.max_reject_rate
        {
            return Some("risk-scored reject-rate limit breached".to_string());
        }

        None
    }
}

#[async_trait]
impl RiskManager for StrictRiskManager {
    async fn evaluate(&self, intents: Vec<TradeIntent>, context: &RiskContext) -> Result<Vec<RiskDecision>> {
        let resulting_mode = self.resulting_mode(context.runtime_state);
        let runtime_block = !context.runtime_state.allows_new_orders();
        let health_block = self.is_health_blocking(&context.health);
        let drawdown_guard = self.check_drawdown_guard(context);
        let reject_rate_guard = self.check_reject_rate_guard(context);
        let (mut projected_open_buys, mut projected_open_sells) =
            projected_open_quantities(&context.open_orders);
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
                decisions.push(block(intent, "missing per-symbol risk limits", resulting_mode));
                continue;
            };

            if runtime_block {
                decisions.push(block(
                    intent,
                    "runtime state does not allow new orders",
                    resulting_mode,
                ));
                continue;
            }

            if health_block {
                decisions.push(block(
                    intent,
                    &format!("health is not healthy: {:?}", context.health.overall_state),
                    resulting_mode,
                ));
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

            let current_position = self
                .inventory_for_symbol(&context.inventory, intent.symbol)
                .map(|inventory| inventory.base_position)
                .unwrap_or(Decimal::ZERO);

            if projected_open_orders
                .get(&intent.symbol)
                .copied()
                .unwrap_or_default()
                >= symbol_limits.max_open_orders as usize
            {
                decisions.push(block(intent, "max open orders reached for symbol", resulting_mode));
                continue;
            }

            if self.notional_for_intent(&intent, context) > symbol_limits.max_quote_notional {
                decisions.push(block(
                    intent,
                    "intent notional exceeds per-symbol limit",
                    resulting_mode,
                ));
                continue;
            }

            let mut candidate_open_buys = projected_open_buys.clone();
            let mut candidate_open_sells = projected_open_sells.clone();
            match intent.side {
                domain::Side::Buy => {
                    *candidate_open_buys.entry(intent.symbol).or_default() += intent.quantity;
                }
                domain::Side::Sell => {
                    *candidate_open_sells.entry(intent.symbol).or_default() += intent.quantity;
                }
            }

            let projected_sell_inventory = projected_inventory_after_sells(
                current_position,
                candidate_open_sells
                    .get(&intent.symbol)
                    .copied()
                    .unwrap_or(Decimal::ZERO),
            );
            if projected_sell_inventory < Decimal::ZERO {
                decisions.push(block(
                    intent,
                    "projected sell quantity exceeds available base inventory",
                    resulting_mode,
                ));
                continue;
            }

            if self.projected_symbol_exposure_quote(
                &context.inventory,
                &candidate_open_buys,
                &candidate_open_sells,
                context,
                intent.symbol,
            )
                > symbol_limits.max_quote_notional
                && !self.would_reduce_inventory(current_position, &intent)
            {
                decisions.push(block(
                    intent,
                    "projected symbol quote exposure exceeds limit",
                    resulting_mode,
                ));
                continue;
            }

            let projected_symbol_inventory = worst_case_symbol_inventory(
                current_position,
                candidate_open_buys
                    .get(&intent.symbol)
                    .copied()
                    .unwrap_or(Decimal::ZERO),
                candidate_open_sells
                    .get(&intent.symbol)
                    .copied()
                    .unwrap_or(Decimal::ZERO),
            );
            if projected_symbol_inventory > symbol_limits.max_inventory_base
                && !self.would_reduce_inventory(current_position, &intent)
            {
                decisions.push(block(intent, "intent would breach max inventory", resulting_mode));
                continue;
            }

            if self.total_projected_exposure_quote(
                &context.inventory,
                &candidate_open_buys,
                &candidate_open_sells,
                context,
            )
                > self.limits.max_total_exposure_quote
                && !self.would_reduce_inventory(current_position, &intent)
            {
                decisions.push(block(
                    intent,
                    "projected total quote exposure exceeds limit",
                    resulting_mode,
                ));
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

                    match approved_intent.side {
                        domain::Side::Buy => {
                            *projected_open_buys.entry(intent.symbol).or_default() +=
                                approved_intent.quantity;
                        }
                        domain::Side::Sell => {
                            *projected_open_sells.entry(intent.symbol).or_default() +=
                                approved_intent.quantity;
                        }
                    }
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

fn block(intent: TradeIntent, reason: &str, resulting_mode: RiskMode) -> RiskDecision {
    RiskDecision {
        action: RiskAction::Block,
        original_intent: intent,
        approved_intent: None,
        reason: reason.to_string(),
        resulting_mode,
        decided_at: now_utc(),
    }
}

fn projected_open_quantities(
    open_orders: &[OpenOrder],
) -> (HashMap<Symbol, Decimal>, HashMap<Symbol, Decimal>) {
    let mut buys = HashMap::new();
    let mut sells = HashMap::new();

    for order in open_orders.iter().filter(|order| order.status.is_open()) {
        let remaining_quantity = remaining_open_quantity(order);
        if remaining_quantity <= Decimal::ZERO {
            continue;
        }

        match order.side {
            domain::Side::Buy => {
                *buys.entry(order.symbol).or_default() += remaining_quantity;
            }
            domain::Side::Sell => {
                *sells.entry(order.symbol).or_default() += remaining_quantity;
            }
        }
    }

    (buys, sells)
}

fn remaining_open_quantity(order: &OpenOrder) -> Decimal {
    (order.original_quantity - order.executed_quantity).max(Decimal::ZERO)
}

fn projected_inventory_after_sells(position: Decimal, pending_sells: Decimal) -> Decimal {
    position - pending_sells
}

fn worst_case_symbol_inventory(
    position: Decimal,
    pending_buys: Decimal,
    pending_sells: Decimal,
) -> Decimal {
    let upper = (position + pending_buys).abs();
    let lower = (position - pending_sells).abs();
    if upper >= lower {
        upper
    } else {
        lower
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{Balance, RuntimeState, StrategyKind, SystemHealth};

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
            max_total_exposure_quote: Decimal::from(5_000u32),
            max_reject_rate: Decimal::from_str_exact("0.30").unwrap(),
            stale_market_data_ms: 1_000,
            stale_account_events_ms: 1_000,
            max_clock_drift_ms: 100,
        }
    }

    fn empty_pnl(now: common::Timestamp) -> PnlSnapshot {
        PnlSnapshot {
            realized_pnl_quote: Decimal::ZERO,
            unrealized_pnl_quote: Decimal::ZERO,
            net_pnl_quote: Decimal::ZERO,
            fees_quote: Decimal::ZERO,
            daily_pnl_quote: Decimal::ZERO,
            peak_net_pnl_quote: Decimal::ZERO,
            drawdown_quote: Decimal::ZERO,
            per_symbol: Vec::new(),
            updated_at: now,
        }
    }

    fn inventory_snapshot(symbol: Symbol, base_position: Decimal, mark_price: Decimal) -> InventorySnapshot {
        InventorySnapshot {
            symbol,
            base_position,
            quote_position: Decimal::ZERO,
            mark_price: Some(mark_price),
            average_entry_price: Some(mark_price),
            updated_at: now_utc(),
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
            pnl: empty_pnl(now),
            execution_stats: ExecutionStats::empty(now),
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
            expected_edge_bps: Decimal::from(5u32),
            expected_fee_bps: Decimal::ONE,
            expected_slippage_bps: Decimal::ONE,
            edge_after_cost_bps: Decimal::from(3u32),
            reason: "test".to_string(),
            created_at: now_utc(),
            expires_at: None,
        }
    }

    fn sample_open_order(side: domain::Side, quantity: &str) -> OpenOrder {
        OpenOrder {
            client_order_id: format!("open-{side:?}-{quantity}"),
            exchange_order_id: Some("exchange-1".to_string()),
            symbol: Symbol::BtcUsdc,
            side,
            price: Some(Decimal::from(60_000u32)),
            original_quantity: Decimal::from_str_exact(quantity).unwrap(),
            executed_quantity: Decimal::ZERO,
            status: domain::OrderStatus::New,
            reduce_only: false,
            updated_at: now_utc(),
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
    async fn blocks_when_projected_symbol_exposure_exceeds_limit() {
        let manager = StrictRiskManager::new(sample_limits());
        let mut context = sample_context();
        context.inventory.insert(
            Symbol::BtcUsdc,
            inventory_snapshot(
                Symbol::BtcUsdc,
                Decimal::from_str_exact("0.010").unwrap(),
                Decimal::from(60_000u32),
            ),
        );

        let mut intent = sample_intent();
        intent.quantity = Decimal::from_str_exact("0.010").unwrap();

        let decisions = manager.evaluate(vec![intent], &context).await.unwrap();

        assert_eq!(decisions[0].action, RiskAction::Block);
        assert_eq!(decisions[0].reason, "projected symbol quote exposure exceeds limit");
    }

    #[tokio::test]
    async fn blocks_when_projected_total_exposure_exceeds_limit() {
        let mut limits = sample_limits();
        limits.max_total_exposure_quote = Decimal::from(1_000u32);
        limits.per_symbol.push(domain::SymbolRiskLimits {
            symbol: Symbol::EthUsdc,
            max_inventory_base: Decimal::from(5u32),
            soft_inventory_base: Decimal::from(2u32),
            max_quote_notional: Decimal::from(10_000u32),
            max_open_orders: 2,
        });

        let manager = StrictRiskManager::new(limits);
        let mut context = sample_context();
        context.mid_prices.insert(Symbol::EthUsdc, Decimal::from(2_000u32));
        context.inventory.insert(
            Symbol::BtcUsdc,
            inventory_snapshot(
                Symbol::BtcUsdc,
                Decimal::from_str_exact("0.010").unwrap(),
                Decimal::from(60_000u32),
            ),
        );
        context.inventory.insert(
            Symbol::EthUsdc,
            inventory_snapshot(
                Symbol::EthUsdc,
                Decimal::from_str_exact("0.20").unwrap(),
                Decimal::from(2_000u32),
            ),
        );

        let mut intent = sample_intent();
        intent.symbol = Symbol::EthUsdc;
        intent.quantity = Decimal::from_str_exact("0.05").unwrap();
        intent.limit_price = Some(Decimal::from(2_000u32));

        let decisions = manager.evaluate(vec![intent], &context).await.unwrap();

        assert_eq!(decisions[0].action, RiskAction::Block);
        assert_eq!(decisions[0].reason, "projected total quote exposure exceeds limit");
    }

    #[tokio::test]
    async fn forces_reduce_only_when_inventory_is_excessive() {
        let manager = StrictRiskManager::new(sample_limits());
        let mut context = sample_context();
        context.inventory.insert(
            Symbol::BtcUsdc,
            inventory_snapshot(
                Symbol::BtcUsdc,
                Decimal::from_str_exact("0.095").unwrap(),
                Decimal::from(60_000u32),
            ),
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

    #[tokio::test]
    async fn blocks_when_daily_loss_limit_is_breached() {
        let manager = StrictRiskManager::new(sample_limits());
        let mut context = sample_context();
        context.pnl.daily_pnl_quote = Decimal::from(-150i32);

        let decisions = manager.evaluate(vec![sample_intent()], &context).await.unwrap();
        assert_eq!(decisions[0].action, RiskAction::Block);
    }

    #[tokio::test]
    async fn blocks_when_existing_open_buys_already_consume_inventory_headroom() {
        let mut limits = sample_limits();
        limits.per_symbol[0].max_inventory_base = Decimal::from_str_exact("0.015").unwrap();
        limits.per_symbol[0].max_quote_notional = Decimal::from(2_000u32);
        let manager = StrictRiskManager::new(limits);
        let mut context = sample_context();
        context.open_orders.push(sample_open_order(domain::Side::Buy, "0.010"));

        let mut intent = sample_intent();
        intent.quantity = Decimal::from_str_exact("0.010").unwrap();

        let decisions = manager.evaluate(vec![intent], &context).await.unwrap();

        assert_eq!(decisions[0].action, RiskAction::Block);
        assert_eq!(decisions[0].reason, "intent would breach max inventory");
    }

    #[tokio::test]
    async fn blocks_sell_that_would_exceed_base_left_after_existing_asks() {
        let manager = StrictRiskManager::new(sample_limits());
        let mut context = sample_context();
        context.inventory.insert(
            Symbol::BtcUsdc,
            inventory_snapshot(
                Symbol::BtcUsdc,
                Decimal::from_str_exact("0.010").unwrap(),
                Decimal::from(60_000u32),
            ),
        );
        context
            .open_orders
            .push(sample_open_order(domain::Side::Sell, "0.010"));

        let mut intent = sample_intent();
        intent.side = domain::Side::Sell;
        intent.quantity = Decimal::from_str_exact("0.001").unwrap();

        let decisions = manager.evaluate(vec![intent], &context).await.unwrap();

        assert_eq!(decisions[0].action, RiskAction::Block);
        assert_eq!(
            decisions[0].reason,
            "projected sell quantity exceeds available base inventory"
        );
    }
}
