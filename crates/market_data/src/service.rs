use crate::OrderBookCache;
use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, Timestamp, now_utc};
use domain::{
    HealthState, MarketDataFreshness, MarketDataStatus, MarketSnapshot, MarketTrade, OrderBookDelta,
    OrderBookSnapshot, Symbol,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

const SNAPSHOT_DEPTH: usize = 50;
const MAX_RECENT_TRADES: usize = 256;
const TRADE_FLOW_WINDOW_SECS: i64 = 10;

#[async_trait]
pub trait MarketDataService: Send + Sync {
    async fn apply_orderbook_snapshot(&self, snapshot: OrderBookSnapshot) -> Result<()>;
    async fn apply_orderbook_delta(&self, delta: OrderBookDelta) -> Result<()>;
    async fn apply_trade(&self, trade: MarketTrade) -> Result<()>;
    async fn set_market_ws_reconnect_counter(&self, reconnect_count: u64) -> Result<()>;
    async fn snapshot(&self, symbol: Symbol) -> Option<MarketSnapshot>;
    async fn freshness(&self, symbol: Symbol, stale_after_ms: u64) -> Option<MarketDataFreshness>;
    async fn needs_resync(&self, symbol: Symbol) -> bool;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryMarketDataService {
    books: Arc<RwLock<HashMap<Symbol, OrderBookCache>>>,
    trades: Arc<RwLock<HashMap<Symbol, VecDeque<MarketTrade>>>>,
    market_ws_reconnect_counter: Arc<RwLock<u64>>,
}

#[async_trait]
impl MarketDataService for InMemoryMarketDataService {
    async fn apply_orderbook_snapshot(&self, snapshot: OrderBookSnapshot) -> Result<()> {
        let mut books = self.books.write().await;
        books
            .entry(snapshot.symbol)
            .or_insert_with(|| OrderBookCache::new(snapshot.symbol))
            .apply_snapshot(snapshot);
        Ok(())
    }

    async fn apply_orderbook_delta(&self, delta: OrderBookDelta) -> Result<()> {
        let mut books = self.books.write().await;
        books
            .entry(delta.symbol)
            .or_insert_with(|| OrderBookCache::new(delta.symbol))
            .apply_delta(delta)?;
        Ok(())
    }

    async fn apply_trade(&self, trade: MarketTrade) -> Result<()> {
        let mut trades = self.trades.write().await;
        let queue = trades.entry(trade.symbol).or_default();
        queue.push_back(trade);
        while queue.len() > MAX_RECENT_TRADES {
            queue.pop_front();
        }
        Ok(())
    }

    async fn set_market_ws_reconnect_counter(&self, reconnect_count: u64) -> Result<()> {
        *self.market_ws_reconnect_counter.write().await = reconnect_count;
        Ok(())
    }

    async fn snapshot(&self, symbol: Symbol) -> Option<MarketSnapshot> {
        let books = self.books.read().await;
        let trades = self.trades.read().await;
        let reconnect_counter = *self.market_ws_reconnect_counter.read().await;
        let book = books.get(&symbol)?;
        let orderbook = book.snapshot(SNAPSHOT_DEPTH);
        let best_bid_ask = book.best_bid_ask();
        let recent_trades = trades
            .get(&symbol)
            .map(|queue| queue.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let last_trade = recent_trades.last().cloned();
        let status = MarketDataStatusBuilder::new(
            orderbook.as_ref(),
            &recent_trades,
            book.last_event_latency_ms(),
            book.book_crossed(),
            book.needs_resync(),
            reconnect_counter,
        )
        .build(1_000);

        Some(MarketSnapshot {
            symbol,
            best_bid_ask,
            orderbook,
            last_trade,
            recent_trades,
            status,
        })
    }

    async fn freshness(&self, symbol: Symbol, stale_after_ms: u64) -> Option<MarketDataFreshness> {
        let books = self.books.read().await;
        let trades = self.trades.read().await;
        let reconnect_counter = *self.market_ws_reconnect_counter.read().await;
        let book = books.get(&symbol)?;
        let orderbook = book.snapshot(SNAPSHOT_DEPTH);
        let recent_trades = trades
            .get(&symbol)
            .map(|queue| queue.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let status = MarketDataStatusBuilder::new(
            orderbook.as_ref(),
            &recent_trades,
            book.last_event_latency_ms(),
            book.book_crossed(),
            book.needs_resync(),
            reconnect_counter,
        )
        .build(stale_after_ms);

        Some(MarketDataFreshness {
            state: if status.is_stale || status.needs_resync || status.book_crossed {
                HealthState::Stale
            } else {
                HealthState::Healthy
            },
            last_orderbook_update_at: status.last_orderbook_update_at,
            last_trade_update_at: status.last_trade_update_at,
        })
    }

    async fn needs_resync(&self, symbol: Symbol) -> bool {
        self.books
            .read()
            .await
            .get(&symbol)
            .map(|book| book.needs_resync())
            .unwrap_or(true)
    }
}

struct MarketDataStatusBuilder<'a> {
    orderbook: Option<&'a OrderBookSnapshot>,
    recent_trades: &'a [MarketTrade],
    last_event_latency_ms: Option<i64>,
    book_crossed: bool,
    needs_resync: bool,
    reconnect_counter: u64,
}

impl<'a> MarketDataStatusBuilder<'a> {
    fn new(
        orderbook: Option<&'a OrderBookSnapshot>,
        recent_trades: &'a [MarketTrade],
        last_event_latency_ms: Option<i64>,
        book_crossed: bool,
        needs_resync: bool,
        reconnect_counter: u64,
    ) -> Self {
        Self {
            orderbook,
            recent_trades,
            last_event_latency_ms,
            book_crossed,
            needs_resync,
            reconnect_counter,
        }
    }

    fn build(self, stale_after_ms: u64) -> MarketDataStatus {
        let now = now_utc();
        let last_orderbook_update_at = self.orderbook.map(|snapshot| snapshot.observed_at);
        let last_trade_update_at = self.recent_trades.last().map(|trade| trade.received_at);
        let orderbook_stale = is_stale(last_orderbook_update_at, now, stale_after_ms);
        let trade_stale =
            !self.recent_trades.is_empty() && is_stale(last_trade_update_at, now, stale_after_ms * 2);
        let book_age_ms = last_orderbook_update_at
            .map(|timestamp| (now - timestamp).whole_milliseconds() as i64);

        MarketDataStatus {
            is_stale: self.needs_resync || self.book_crossed || orderbook_stale || trade_stale,
            needs_resync: self.needs_resync,
            last_orderbook_update_at,
            last_trade_update_at,
            last_event_latency_ms: self.last_event_latency_ms,
            book_age_ms,
            trade_flow_rate: trade_flow_rate(self.recent_trades, now),
            book_crossed: self.book_crossed,
            ws_reconnect_counter: self.reconnect_counter,
        }
    }
}

fn trade_flow_rate(trades: &[MarketTrade], now: Timestamp) -> Decimal {
    let window_start = now - time::Duration::seconds(TRADE_FLOW_WINDOW_SECS);
    let recent = trades
        .iter()
        .filter(|trade| trade.received_at >= window_start)
        .collect::<Vec<_>>();

    if recent.is_empty() {
        return Decimal::ZERO;
    }

    let quantity = recent
        .iter()
        .fold(Decimal::ZERO, |acc, trade| acc + trade.quantity);
    quantity / Decimal::from(TRADE_FLOW_WINDOW_SECS as i64)
}

fn is_stale(last_update_at: Option<Timestamp>, now: Timestamp, stale_after_ms: u64) -> bool {
    let Some(last_update_at) = last_update_at else {
        return true;
    };
    (now - last_update_at).whole_milliseconds() > stale_after_ms as i128
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::now_utc;
    use domain::{PriceLevel, Side};

    #[tokio::test]
    async fn snapshot_exposes_trade_flow_and_book_age() {
        let service = InMemoryMarketDataService::default();
        let now = now_utc();
        service
            .apply_orderbook_snapshot(OrderBookSnapshot {
                symbol: Symbol::BtcUsdc,
                bids: vec![PriceLevel {
                    price: Decimal::from(60_000u32),
                    quantity: Decimal::from_str_exact("0.5").unwrap(),
                }],
                asks: vec![PriceLevel {
                    price: Decimal::from(60_010u32),
                    quantity: Decimal::from_str_exact("0.4").unwrap(),
                }],
                last_update_id: 1,
                exchange_time: Some(now),
                observed_at: now,
            })
            .await
            .unwrap();
        service
            .apply_trade(MarketTrade {
                symbol: Symbol::BtcUsdc,
                trade_id: "t1".to_string(),
                price: Decimal::from(60_005u32),
                quantity: Decimal::from_str_exact("0.01").unwrap(),
                aggressor_side: Side::Buy,
                event_time: now,
                received_at: now,
            })
            .await
            .unwrap();

        let snapshot = service.snapshot(Symbol::BtcUsdc).await.unwrap();
        assert!(snapshot.status.book_age_ms.is_some());
        assert!(snapshot.status.trade_flow_rate > Decimal::ZERO);
        assert_eq!(snapshot.recent_trades.len(), 1);
    }
}
