use crate::{ExitStage, IntentRole, Symbol};
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
    pub average_entry_price: Option<Decimal>,
    pub position_opened_at: Option<Timestamp>,
    pub last_fill_at: Option<Timestamp>,
    pub first_reduce_at: Option<Timestamp>,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolBudget {
    pub symbol: Symbol,
    pub max_quote_notional: Decimal,
    pub reserved_quote_notional: Decimal,
    pub soft_inventory_base: Decimal,
    pub max_inventory_base: Decimal,
    pub neutralization_clip_fraction: Decimal,
    pub reduce_only_trigger_ratio: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolPnlSnapshot {
    pub symbol: Symbol,
    pub position_base: Decimal,
    pub average_entry_price: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub realized_pnl_quote: Decimal,
    pub unrealized_pnl_quote: Decimal,
    pub net_pnl_quote: Decimal,
    pub fees_quote: Decimal,
    pub peak_net_pnl_quote: Decimal,
    pub drawdown_quote: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionExitEvent {
    pub symbol: Symbol,
    pub opened_at: Timestamp,
    pub first_exit_at: Option<Timestamp>,
    pub closed_at: Timestamp,
    pub hold_time_ms: i64,
    pub time_to_first_exit_ms: Option<i64>,
    pub intent_role: Option<IntentRole>,
    pub exit_stage: Option<ExitStage>,
    pub exit_reason: Option<String>,
    pub passive_exit: bool,
    pub aggressive_exit: bool,
    pub forced_unwind: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PnlSnapshot {
    pub realized_pnl_quote: Decimal,
    pub unrealized_pnl_quote: Decimal,
    pub net_pnl_quote: Decimal,
    pub fees_quote: Decimal,
    pub daily_pnl_quote: Decimal,
    pub peak_net_pnl_quote: Decimal,
    pub drawdown_quote: Decimal,
    pub per_symbol: Vec<SymbolPnlSnapshot>,
    pub updated_at: Timestamp,
}
