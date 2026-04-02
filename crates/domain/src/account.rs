use crate::Symbol;
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Balance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

impl Balance {
    pub fn total(&self) -> Decimal {
        self.free + self.locked
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub balances: Vec<Balance>,
    pub updated_at: Timestamp,
}

impl AccountSnapshot {
    pub fn balance(&self, asset: &str) -> Option<&Balance> {
        self.balances
            .iter()
            .find(|balance| balance.asset.eq_ignore_ascii_case(asset))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventorySnapshot {
    pub symbol: Symbol,
    pub base_position: Decimal,
    pub quote_position: Decimal,
    pub mark_price: Option<Decimal>,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolBudget {
    pub symbol: Symbol,
    pub max_quote_notional: Decimal,
    pub reserved_quote_notional: Decimal,
}
