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
            average_entry_price: Some(fill.price),
            updated_at: fill.event_time,
        });

        match fill.side {
            Side::Buy => {
                let previous_base = entry.base_position.max(Decimal::ZERO);
                let new_base = previous_base + fill.quantity;
                entry.average_entry_price = if new_base.is_zero() {
                    None
                } else {
                    let previous_cost = previous_base * entry.average_entry_price.unwrap_or(fill.price);
                    Some((previous_cost + fill.quantity * fill.price) / new_base)
                };
                entry.base_position += fill.quantity;
                entry.quote_position -= fill.quantity * fill.price;
            }
            Side::Sell => {
                entry.base_position -= fill.quantity;
                entry.quote_position += fill.quantity * fill.price;
                if entry.base_position <= Decimal::ZERO {
                    entry.average_entry_price = None;
                }
            }
        }

        entry.mark_price = Some(fill.price);
        entry.updated_at = fill.event_time;
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
            budget.reserved_quote_notional = inventory.quote_position.abs();
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
        let usdc_balance = snapshot
            .balance("USDC")
            .map(|balance| balance.total())
            .unwrap_or(Decimal::ZERO);

        let mut inventory = self.inventory.write().await;
        sync_inventory(&mut inventory, Symbol::BtcUsdc, btc_balance, usdc_balance, updated_at);
        sync_inventory(&mut inventory, Symbol::EthUsdc, eth_balance, usdc_balance, updated_at);
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
    quote_position: Decimal,
    updated_at: common::Timestamp,
) {
    let entry = inventory.entry(symbol).or_insert(InventorySnapshot {
        symbol,
        base_position: Decimal::ZERO,
        quote_position,
        mark_price: None,
        average_entry_price: None,
        updated_at,
    });

    entry.base_position = base_position;
    entry.quote_position = quote_position;
    entry.updated_at = updated_at;
}
