use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, Timestamp, now_utc};
use domain::{AccountSnapshot, Balance, FillEvent, InventorySnapshot, Side, Symbol, SymbolBudget};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[async_trait]
pub trait PortfolioService: Send + Sync {
    async fn apply_account_snapshot(&self, snapshot: AccountSnapshot) -> Result<()>;
    async fn apply_account_position(&self, balances: Vec<Balance>, updated_at: Timestamp) -> Result<()>;
    async fn apply_balance_delta(&self, asset: String, delta: Decimal, event_time: Timestamp) -> Result<()>;
    async fn apply_fill(&self, fill: FillEvent) -> Result<()>;
    async fn apply_mark_price(&self, symbol: Symbol, mark_price: Decimal) -> Result<()>;
    async fn inventory(&self, symbol: Symbol) -> Option<InventorySnapshot>;
    async fn account_snapshot(&self) -> Option<AccountSnapshot>;
    async fn budget(&self, symbol: Symbol) -> Option<SymbolBudget>;
}

#[derive(Debug, Clone)]
pub struct InMemoryPortfolioService {
    account: Arc<RwLock<Option<AccountSnapshot>>>,
    inventory: Arc<RwLock<HashMap<Symbol, InventorySnapshot>>>,
    budgets: Arc<RwLock<HashMap<Symbol, SymbolBudget>>>,
}

impl InMemoryPortfolioService {
    pub fn new(budgets: impl IntoIterator<Item = SymbolBudget>) -> Self {
        Self {
            account: Arc::new(RwLock::new(None)),
            inventory: Arc::new(RwLock::new(HashMap::new())),
            budgets: Arc::new(RwLock::new(
                budgets
                    .into_iter()
                    .map(|budget| (budget.symbol, budget))
                    .collect(),
            )),
        }
    }
}

impl Default for InMemoryPortfolioService {
    fn default() -> Self {
        Self::new(Vec::<SymbolBudget>::new())
    }
}

#[async_trait]
impl PortfolioService for InMemoryPortfolioService {
    async fn apply_account_snapshot(&self, snapshot: AccountSnapshot) -> Result<()> {
        let updated_at = snapshot.updated_at;
        *self.account.write().await = Some(snapshot.clone());
        self.sync_inventory_from_snapshot(&snapshot, updated_at).await;
        Ok(())
    }

    async fn apply_account_position(&self, balances: Vec<Balance>, updated_at: Timestamp) -> Result<()> {
        let snapshot = {
            let mut account = self.account.write().await;
            let mut next = account.clone().unwrap_or(AccountSnapshot {
                balances: Vec::new(),
                updated_at,
            });

            for balance in balances {
                if let Some(existing) = next
                    .balances
                    .iter_mut()
                    .find(|existing| existing.asset.eq_ignore_ascii_case(&balance.asset))
                {
                    *existing = balance;
                } else {
                    next.balances.push(balance);
                }
            }

            next.updated_at = updated_at;
            *account = Some(next.clone());
            next
        };

        self.sync_inventory_from_snapshot(&snapshot, updated_at).await;
        Ok(())
    }

    async fn apply_balance_delta(&self, asset: String, delta: Decimal, event_time: Timestamp) -> Result<()> {
        let snapshot = {
            let mut account = self.account.write().await;
            let mut next = account.clone().unwrap_or(AccountSnapshot {
                balances: Vec::new(),
                updated_at: event_time,
            });

            if let Some(balance) = next
                .balances
                .iter_mut()
                .find(|balance| balance.asset.eq_ignore_ascii_case(&asset))
            {
                balance.free += delta;
            } else {
                next.balances.push(Balance {
                    asset,
                    free: delta,
                    locked: Decimal::ZERO,
                });
            }

            next.updated_at = event_time;
            *account = Some(next.clone());
            next
        };

        self.sync_inventory_from_snapshot(&snapshot, event_time).await;
        Ok(())
    }

    async fn apply_fill(&self, fill: FillEvent) -> Result<()> {
        let mut inventory = self.inventory.write().await;
        let entry = inventory.entry(fill.symbol).or_insert(InventorySnapshot {
            symbol: fill.symbol,
            base_position: Decimal::ZERO,
            quote_position: Decimal::ZERO,
            mark_price: Some(fill.price),
            average_entry_price: None,
            position_opened_at: None,
            last_fill_at: None,
            first_reduce_at: None,
            updated_at: fill.event_time,
        });
        apply_fill_to_inventory(entry, &fill);
        Ok(())
    }

    async fn apply_mark_price(&self, symbol: Symbol, mark_price: Decimal) -> Result<()> {
        let mut inventory = self.inventory.write().await;
        let entry = inventory.entry(symbol).or_insert(InventorySnapshot {
            symbol,
            base_position: Decimal::ZERO,
            quote_position: Decimal::ZERO,
            mark_price: Some(mark_price),
            average_entry_price: None,
            position_opened_at: None,
            last_fill_at: None,
            first_reduce_at: None,
            updated_at: now_utc(),
        });
        entry.mark_price = Some(mark_price);
        entry.updated_at = now_utc();
        Ok(())
    }

    async fn inventory(&self, symbol: Symbol) -> Option<InventorySnapshot> {
        self.inventory.read().await.get(&symbol).cloned()
    }

    async fn account_snapshot(&self) -> Option<AccountSnapshot> {
        self.account.read().await.clone()
    }

    async fn budget(&self, symbol: Symbol) -> Option<SymbolBudget> {
        let inventory = self.inventory(symbol).await;
        let mut budget = self.budgets.read().await.get(&symbol).cloned()?;
        if let Some(inventory) = inventory {
            let reference_price = inventory
                .mark_price
                .or(inventory.average_entry_price)
                .unwrap_or(Decimal::ZERO);
            let tracked_quote_notional = inventory.quote_position.abs();
            let base_exposure_quote = inventory.base_position.abs() * reference_price;
            let raw_reserved_quote = tracked_quote_notional.max(base_exposure_quote);
            let soft_limit_quote = budget.soft_inventory_base * reference_price;
            let reduce_only_limit_quote =
                budget.max_inventory_base * budget.reduce_only_trigger_ratio * reference_price;

            budget.reserved_quote_notional = reserve_quote_notional(
                raw_reserved_quote,
                soft_limit_quote,
                reduce_only_limit_quote,
                budget.max_quote_notional,
                budget.neutralization_clip_fraction,
            );
        }
        Some(budget)
    }
}

impl InMemoryPortfolioService {
    async fn sync_inventory_from_snapshot(&self, snapshot: &AccountSnapshot, updated_at: Timestamp) {
        let btc_balance = snapshot
            .balance("BTC")
            .map(|balance| balance.total())
            .unwrap_or(Decimal::ZERO);
        let eth_balance = snapshot
            .balance("ETH")
            .map(|balance| balance.total())
            .unwrap_or(Decimal::ZERO);

        let mut inventory = self.inventory.write().await;
        sync_inventory(&mut inventory, Symbol::BtcUsdc, btc_balance, updated_at);
        sync_inventory(&mut inventory, Symbol::EthUsdc, eth_balance, updated_at);
    }
}

#[allow(dead_code)]
fn _placeholder_timestamp() -> common::Timestamp {
    now_utc()
}

fn sync_inventory(
    inventory: &mut HashMap<Symbol, InventorySnapshot>,
    symbol: Symbol,
    base_position: Decimal,
    updated_at: common::Timestamp,
) {
    let entry = inventory.entry(symbol).or_insert(InventorySnapshot {
        symbol,
        base_position: Decimal::ZERO,
        quote_position: Decimal::ZERO,
        mark_price: None,
        average_entry_price: None,
        position_opened_at: None,
        last_fill_at: None,
        first_reduce_at: None,
        updated_at,
    });

    let previous_base = entry.base_position;
    let previous_average_entry = entry.average_entry_price;
    entry.base_position = base_position;
    entry.quote_position = match previous_average_entry {
        Some(average_entry_price) if same_nonzero_direction(previous_base, base_position) => {
            signed_quote_position(base_position, average_entry_price)
        }
        _ => Decimal::ZERO,
    };
    entry.average_entry_price = match previous_average_entry {
        Some(average_entry_price) if same_nonzero_direction(previous_base, base_position) => {
            Some(average_entry_price)
        }
        _ => None,
    };
    if entry.base_position.is_zero() {
        entry.quote_position = Decimal::ZERO;
        entry.average_entry_price = None;
        entry.position_opened_at = None;
        entry.last_fill_at = None;
        entry.first_reduce_at = None;
    } else if same_nonzero_direction(previous_base, entry.base_position) {
        entry.position_opened_at = entry.position_opened_at.or(Some(updated_at));
    } else {
        entry.position_opened_at = Some(updated_at);
        entry.first_reduce_at = None;
    }
    entry.updated_at = updated_at;
}

fn apply_fill_to_inventory(entry: &mut InventorySnapshot, fill: &FillEvent) {
    let previous_base = entry.base_position;
    let signed_quantity = signed_fill_quantity(fill.side, fill.quantity);
    let next_base = previous_base + signed_quantity;
    let previous_average_entry = entry.average_entry_price;
    let was_flat = previous_base.is_zero();
    let position_reversed =
        !was_flat && !next_base.is_zero() && direction(previous_base) != direction(next_base);
    let reduced_existing_position = !was_flat
        && !position_reversed
        && direction(previous_base) != direction(signed_quantity)
        && next_base.abs() < previous_base.abs();

    entry.base_position = next_base;
    entry.average_entry_price = match previous_average_entry {
        Some(average_entry_price) if same_nonzero_direction(previous_base, signed_quantity) => {
            let previous_abs = previous_base.abs();
            let next_abs = next_base.abs();
            if next_abs.is_zero() {
                None
            } else {
                let weighted_cost =
                    previous_abs * average_entry_price + fill.quantity * fill.price;
                Some(weighted_cost / next_abs)
            }
        }
        Some(average_entry_price) if same_nonzero_direction(previous_base, next_base) => {
            Some(average_entry_price)
        }
        _ if next_base.is_zero() => None,
        _ => Some(fill.price),
    };
    entry.quote_position = match entry.average_entry_price {
        Some(average_entry_price) => signed_quote_position(next_base, average_entry_price),
        None => Decimal::ZERO,
    };
    if next_base.is_zero() {
        entry.position_opened_at = None;
        entry.first_reduce_at = None;
    } else if was_flat || position_reversed {
        entry.position_opened_at = Some(fill.event_time);
        entry.first_reduce_at = None;
    } else if reduced_existing_position && entry.first_reduce_at.is_none() {
        entry.first_reduce_at = Some(fill.event_time);
    }
    entry.last_fill_at = Some(fill.event_time);
    entry.mark_price = Some(fill.price);
    entry.updated_at = fill.event_time;
}

fn signed_fill_quantity(side: Side, quantity: Decimal) -> Decimal {
    match side {
        Side::Buy => quantity,
        Side::Sell => -quantity,
    }
}

fn signed_quote_position(base_position: Decimal, average_entry_price: Decimal) -> Decimal {
    -base_position * average_entry_price
}

fn same_nonzero_direction(left: Decimal, right: Decimal) -> bool {
    direction(left) != 0 && direction(left) == direction(right)
}

fn direction(value: Decimal) -> i8 {
    if value > Decimal::ZERO {
        1
    } else if value < Decimal::ZERO {
        -1
    } else {
        0
    }
}

fn reserve_quote_notional(
    raw_reserved_quote: Decimal,
    soft_limit_quote: Decimal,
    reduce_only_limit_quote: Decimal,
    max_quote_notional: Decimal,
    neutralization_clip_fraction: Decimal,
) -> Decimal {
    if raw_reserved_quote <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    let max_quote_notional = max_quote_notional.max(Decimal::ZERO);
    let clipped_fraction = neutralization_clip_fraction
        .max(Decimal::ZERO)
        .min(Decimal::ONE);
    if max_quote_notional.is_zero() {
        return raw_reserved_quote;
    }

    if reduce_only_limit_quote > Decimal::ZERO && raw_reserved_quote >= reduce_only_limit_quote {
        return max_quote_notional;
    }

    if soft_limit_quote > Decimal::ZERO && raw_reserved_quote > soft_limit_quote {
        return (raw_reserved_quote
            + (raw_reserved_quote - soft_limit_quote) * clipped_fraction)
            .min(max_quote_notional);
    }

    raw_reserved_quote.min(max_quote_notional)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(raw: &str) -> Decimal {
        Decimal::from_str_exact(raw).unwrap()
    }

    fn sample_budget() -> SymbolBudget {
        SymbolBudget {
            symbol: Symbol::BtcUsdc,
            max_quote_notional: dec("500"),
            reserved_quote_notional: Decimal::ZERO,
            soft_inventory_base: dec("0.020"),
            max_inventory_base: dec("0.050"),
            neutralization_clip_fraction: dec("0.20"),
            reduce_only_trigger_ratio: dec("0.90"),
        }
    }

    fn sample_fill(side: Side, quantity: &str, price: &str, event_offset_secs: i64) -> FillEvent {
        FillEvent {
            trade_id: format!("{side:?}-{quantity}-{price}-{event_offset_secs}"),
            order_id: "order-1".to_string(),
            symbol: Symbol::BtcUsdc,
            side,
            price: dec(price),
            quantity: dec(quantity),
            fee: Decimal::ZERO,
            fee_asset: "USDC".to_string(),
            fee_quote: Some(Decimal::ZERO),
            liquidity_side: domain::LiquiditySide::Maker,
            event_time: now_utc(),
        }
    }

    #[tokio::test]
    async fn fill_sequence_keeps_position_coherent_through_add_reduce_and_reverse() {
        let service = InMemoryPortfolioService::default();
        service
            .apply_fill(sample_fill(Side::Buy, "1.0", "100", 0))
            .await
            .unwrap();
        service
            .apply_fill(sample_fill(Side::Buy, "1.0", "110", 1))
            .await
            .unwrap();
        service
            .apply_fill(sample_fill(Side::Sell, "1.5", "120", 2))
            .await
            .unwrap();
        service
            .apply_fill(sample_fill(Side::Sell, "1.0", "90", 3))
            .await
            .unwrap();

        let inventory = service.inventory(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(inventory.base_position, dec("-0.5"));
        assert_eq!(inventory.average_entry_price, Some(dec("90")));
        assert_eq!(inventory.quote_position, dec("45.0"));
        assert_eq!(inventory.mark_price, Some(dec("90")));
    }

    #[tokio::test]
    async fn reduction_and_flatten_reset_inventory_cleanly() {
        let service = InMemoryPortfolioService::default();
        service
            .apply_fill(sample_fill(Side::Buy, "1.0", "100", 0))
            .await
            .unwrap();
        service
            .apply_fill(sample_fill(Side::Sell, "0.4", "101", 1))
            .await
            .unwrap();

        let reduced = service.inventory(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(reduced.base_position, dec("0.6"));
        assert_eq!(reduced.average_entry_price, Some(dec("100")));
        assert_eq!(reduced.quote_position, dec("-60.0"));

        service
            .apply_fill(sample_fill(Side::Sell, "0.6", "102", 2))
            .await
            .unwrap();

        let flattened = service.inventory(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(flattened.base_position, Decimal::ZERO);
        assert_eq!(flattened.quote_position, Decimal::ZERO);
        assert_eq!(flattened.average_entry_price, None);
    }

    #[tokio::test]
    async fn soft_inventory_zone_adds_neutralization_pressure_to_budget() {
        let service = InMemoryPortfolioService::new([sample_budget()]);
        service
            .apply_fill(sample_fill(Side::Buy, "0.030", "10000", 0))
            .await
            .unwrap();
        service
            .apply_mark_price(Symbol::BtcUsdc, dec("10000"))
            .await
            .unwrap();

        let budget = service.budget(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(budget.reserved_quote_notional, dec("320.00000"));
        assert!(budget.reserved_quote_notional > dec("300"));
        assert!(budget.reserved_quote_notional < budget.max_quote_notional);
    }

    #[tokio::test]
    async fn near_max_inventory_clamps_budget_to_hard_limit() {
        let service = InMemoryPortfolioService::new([sample_budget()]);
        service
            .apply_fill(sample_fill(Side::Buy, "0.046", "10000", 0))
            .await
            .unwrap();
        service
            .apply_mark_price(Symbol::BtcUsdc, dec("10000"))
            .await
            .unwrap();

        let budget = service.budget(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(budget.reserved_quote_notional, budget.max_quote_notional);
    }

    #[tokio::test]
    async fn account_snapshot_reconciles_flattened_inventory_without_stale_entry_price() {
        let service = InMemoryPortfolioService::new([sample_budget()]);
        service
            .apply_fill(sample_fill(Side::Buy, "0.010", "10000", 0))
            .await
            .unwrap();

        service
            .apply_account_snapshot(AccountSnapshot {
                balances: vec![
                    Balance {
                        asset: "BTC".to_string(),
                        free: Decimal::ZERO,
                        locked: Decimal::ZERO,
                    },
                    Balance {
                        asset: "USDC".to_string(),
                        free: dec("6900"),
                        locked: Decimal::ZERO,
                    },
                ],
                updated_at: now_utc(),
            })
            .await
            .unwrap();

        let flattened = service.inventory(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(flattened.base_position, Decimal::ZERO);
        assert_eq!(flattened.quote_position, Decimal::ZERO);
        assert_eq!(flattened.average_entry_price, None);

        service
            .apply_fill(sample_fill(Side::Buy, "0.005", "10020", 1))
            .await
            .unwrap();
        service
            .apply_account_position(
                vec![Balance {
                    asset: "BTC".to_string(),
                    free: dec("0.005"),
                    locked: Decimal::ZERO,
                }],
                now_utc(),
            )
            .await
            .unwrap();

        let reopened = service.inventory(Symbol::BtcUsdc).await.unwrap();
        assert_eq!(reopened.base_position, dec("0.005"));
        assert_eq!(reopened.average_entry_price, Some(dec("10020")));
        assert_eq!(reopened.quote_position, dec("-50.100"));
    }
}
