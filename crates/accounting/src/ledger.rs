use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, Timestamp, now_utc};
use domain::{FillEvent, PnlSnapshot, Symbol, SymbolPnlSnapshot};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub label: String,
    pub occurred_at: Timestamp,
    pub net_pnl_quote: Decimal,
}

#[derive(Debug, Clone, Default)]
struct SymbolLedgerState {
    position_base: Decimal,
    average_entry_price: Option<Decimal>,
    mark_price: Option<Decimal>,
    realized_pnl_quote: Decimal,
    unrealized_pnl_quote: Decimal,
    fees_quote: Decimal,
    peak_net_pnl_quote: Decimal,
    drawdown_quote: Decimal,
}

#[derive(Debug, Clone, Default)]
struct AccountingState {
    journal: Vec<LedgerEntry>,
    symbols: HashMap<Symbol, SymbolLedgerState>,
    peak_net_pnl_quote: Decimal,
    drawdown_quote: Decimal,
}

#[async_trait]
pub trait AccountingService: Send + Sync {
    async fn record_fill(&self, fill: &FillEvent) -> Result<()>;
    async fn mark_to_market(&self, symbol: Symbol, mark_price: Decimal, observed_at: Timestamp) -> Result<()>;
    async fn snapshot(&self) -> PnlSnapshot;
    async fn journal(&self) -> Vec<LedgerEntry>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryAccountingService {
    state: Arc<RwLock<AccountingState>>,
}

#[async_trait]
impl AccountingService for InMemoryAccountingService {
    async fn record_fill(&self, fill: &FillEvent) -> Result<()> {
        let mut state = self.state.write().await;
        let fee_quote = fee_to_quote(fill);
        let journal_entry = {
            let entry = state.symbols.entry(fill.symbol).or_default();

            let net_pnl_quote = match fill.side {
                domain::Side::Buy => {
                    let new_position = entry.position_base + fill.quantity;
                    entry.average_entry_price = if new_position.is_zero() {
                        None
                    } else {
                        let previous_cost = entry.position_base.max(Decimal::ZERO)
                            * entry.average_entry_price.unwrap_or(fill.price);
                        Some((previous_cost + fill.quantity * fill.price) / new_position)
                    };
                    entry.position_base = new_position;
                    entry.fees_quote += fee_quote;
                    -fee_quote
                }
                domain::Side::Sell => {
                    let closed_quantity = fill.quantity.min(entry.position_base.max(Decimal::ZERO));
                    let average_entry = entry.average_entry_price.unwrap_or(fill.price);
                    let realized = (fill.price - average_entry) * closed_quantity - fee_quote;
                    entry.position_base = (entry.position_base - fill.quantity).max(Decimal::ZERO);
                    if entry.position_base.is_zero() {
                        entry.average_entry_price = None;
                    }
                    entry.realized_pnl_quote += realized;
                    entry.fees_quote += fee_quote;
                    realized
                }
            };

            recalculate_symbol(entry);
            LedgerEntry {
                label: format!("{:?} {} {} @ {}", fill.side, fill.symbol, fill.quantity, fill.price),
                occurred_at: fill.event_time,
                net_pnl_quote,
            }
        };

        state.journal.push(journal_entry);
        recalculate_global(&mut state);
        Ok(())
    }

    async fn mark_to_market(&self, symbol: Symbol, mark_price: Decimal, _observed_at: Timestamp) -> Result<()> {
        let mut state = self.state.write().await;
        let entry = state.symbols.entry(symbol).or_default();
        entry.mark_price = Some(mark_price);
        recalculate_symbol(entry);
        recalculate_global(&mut state);
        Ok(())
    }

    async fn snapshot(&self) -> PnlSnapshot {
        let state = self.state.read().await;
        build_snapshot(&state)
    }

    async fn journal(&self) -> Vec<LedgerEntry> {
        self.state.read().await.journal.clone()
    }
}

fn fee_to_quote(fill: &FillEvent) -> Decimal {
    if fill.fee_asset.eq_ignore_ascii_case("USDC") {
        fill.fee
    } else if fill
        .fee_asset
        .eq_ignore_ascii_case(base_asset_for_symbol(fill.symbol))
    {
        fill.fee * fill.price
    } else {
        Decimal::ZERO
    }
}

fn base_asset_for_symbol(symbol: Symbol) -> &'static str {
    match symbol {
        Symbol::BtcUsdc => "BTC",
        Symbol::EthUsdc => "ETH",
    }
}

fn recalculate_symbol(entry: &mut SymbolLedgerState) {
    entry.unrealized_pnl_quote = match (entry.position_base, entry.average_entry_price, entry.mark_price) {
        (position, Some(average_entry), Some(mark_price)) if position > Decimal::ZERO => {
            (mark_price - average_entry) * position
        }
        _ => Decimal::ZERO,
    };

    let net = entry.realized_pnl_quote + entry.unrealized_pnl_quote - entry.fees_quote;
    if net > entry.peak_net_pnl_quote {
        entry.peak_net_pnl_quote = net;
    }
    entry.drawdown_quote = (entry.peak_net_pnl_quote - net).max(Decimal::ZERO);
}

fn recalculate_global(state: &mut AccountingState) {
    let net = state.symbols.values().fold(Decimal::ZERO, |acc, entry| {
        acc + entry.realized_pnl_quote + entry.unrealized_pnl_quote - entry.fees_quote
    });
    if net > state.peak_net_pnl_quote {
        state.peak_net_pnl_quote = net;
    }
    state.drawdown_quote = (state.peak_net_pnl_quote - net).max(Decimal::ZERO);
}

fn build_snapshot(state: &AccountingState) -> PnlSnapshot {
    let updated_at = now_utc();
    let today = updated_at.date();
    let realized_pnl_quote = state
        .symbols
        .values()
        .fold(Decimal::ZERO, |acc, entry| acc + entry.realized_pnl_quote);
    let unrealized_pnl_quote = state
        .symbols
        .values()
        .fold(Decimal::ZERO, |acc, entry| acc + entry.unrealized_pnl_quote);
    let fees_quote = state
        .symbols
        .values()
        .fold(Decimal::ZERO, |acc, entry| acc + entry.fees_quote);
    let daily_realized = state
        .journal
        .iter()
        .filter(|entry| entry.occurred_at.date() == today)
        .fold(Decimal::ZERO, |acc, entry| acc + entry.net_pnl_quote);
    let per_symbol = state
        .symbols
        .iter()
        .map(|(symbol, entry)| SymbolPnlSnapshot {
            symbol: *symbol,
            position_base: entry.position_base,
            average_entry_price: entry.average_entry_price,
            mark_price: entry.mark_price,
            realized_pnl_quote: entry.realized_pnl_quote,
            unrealized_pnl_quote: entry.unrealized_pnl_quote,
            net_pnl_quote: entry.realized_pnl_quote + entry.unrealized_pnl_quote - entry.fees_quote,
            fees_quote: entry.fees_quote,
            peak_net_pnl_quote: entry.peak_net_pnl_quote,
            drawdown_quote: entry.drawdown_quote,
        })
        .collect::<Vec<_>>();

    PnlSnapshot {
        realized_pnl_quote,
        unrealized_pnl_quote,
        net_pnl_quote: realized_pnl_quote + unrealized_pnl_quote - fees_quote,
        fees_quote,
        daily_pnl_quote: daily_realized + unrealized_pnl_quote,
        peak_net_pnl_quote: state.peak_net_pnl_quote,
        drawdown_quote: state.drawdown_quote,
        per_symbol,
        updated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{LiquiditySide, Side};

    fn buy_fill(symbol: Symbol, price: u64, qty: &str) -> FillEvent {
        FillEvent {
            trade_id: format!("buy-{price}"),
            order_id: "1".to_string(),
            symbol,
            side: Side::Buy,
            price: Decimal::from(price),
            quantity: Decimal::from_str_exact(qty).unwrap(),
            fee: Decimal::from_str_exact("0.01").unwrap(),
            fee_asset: "USDC".to_string(),
            liquidity_side: LiquiditySide::Maker,
            event_time: now_utc(),
        }
    }

    fn sell_fill(symbol: Symbol, price: u64, qty: &str) -> FillEvent {
        FillEvent {
            side: Side::Sell,
            trade_id: format!("sell-{price}"),
            ..buy_fill(symbol, price, qty)
        }
    }

    #[tokio::test]
    async fn computes_realized_and_unrealized_pnl() {
        let accounting = InMemoryAccountingService::default();
        accounting.record_fill(&buy_fill(Symbol::BtcUsdc, 60_000, "0.010")).await.unwrap();
        accounting
            .mark_to_market(Symbol::BtcUsdc, Decimal::from(60_100u32), now_utc())
            .await
            .unwrap();

        let interim = accounting.snapshot().await;
        assert!(interim.unrealized_pnl_quote > Decimal::ZERO);

        accounting
            .record_fill(&sell_fill(Symbol::BtcUsdc, 60_150, "0.010"))
            .await
            .unwrap();
        let final_snapshot = accounting.snapshot().await;
        assert!(final_snapshot.realized_pnl_quote > Decimal::ZERO);
        assert!(final_snapshot.fees_quote > Decimal::ZERO);
    }
}
