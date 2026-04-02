use crate::OrderBookCache;
use anyhow::Result;
use async_trait::async_trait;
use common::{now_utc, Timestamp};
use domain::{
    HealthState, MarketDataFreshness, MarketSnapshot, MarketTrade, OrderBookDelta,
    OrderBookSnapshot, Symbol,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

const SNAPSHOT_DEPTH: usize = 50;

#[async_trait]
pub trait MarketDataService: Send + Sync {
    async fn apply_orderbook_snapshot(&self, snapshot: OrderBookSnapshot) -> Result<()>;
    async fn apply_orderbook_delta(&self, delta: OrderBookDelta) -> Result<()>;
    async fn apply_trade(&self, trade: MarketTrade) -> Result<()>;
    async fn snapshot(&self, symbol: Symbol) -> Option<MarketSnapshot>;
    async fn freshness(&self, symbol: Symbol, stale_after_ms: u64) -> Option<MarketDataFreshness>;
    async fn needs_resync(&self, symbol: Symbol) -> bool;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryMarketDataService {
    books: Arc<RwLock<HashMap<Symbol, OrderBookCache>>>,
    trades: Arc<RwLock<HashMap<Symbol, MarketTrade>>>,
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
        self.trades.write().await.insert(trade.symbol, trade);
        Ok(())
    }

    async fn snapshot(&self, symbol: Symbol) -> Option<MarketSnapshot> {
        let books = self.books.read().await;
        let trades = self.trades.read().await;
        let book = books.get(&symbol)?;
        let orderbook = book.snapshot(SNAPSHOT_DEPTH);
        let best_bid_ask = book.best_bid_ask();
        let last_trade = trades.get(&symbol).cloned();
        let status = MarketDataStatusBuilder::new(orderbook.as_ref(), last_trade.as_ref(), book.needs_resync())
            .build(1_000);

        Some(MarketSnapshot {
            symbol,
            best_bid_ask,
            orderbook,
            last_trade,
            status,
        })
    }

    async fn freshness(&self, symbol: Symbol, stale_after_ms: u64) -> Option<MarketDataFreshness> {
        let books = self.books.read().await;
        let trades = self.trades.read().await;
        let book = books.get(&symbol)?;
        let orderbook = book.snapshot(SNAPSHOT_DEPTH);
        let last_trade = trades.get(&symbol);
        let status = MarketDataStatusBuilder::new(orderbook.as_ref(), last_trade, book.needs_resync())
            .build(stale_after_ms);

        Some(MarketDataFreshness {
            state: if status.is_stale || status.needs_resync {
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
    trade: Option<&'a MarketTrade>,
    needs_resync: bool,
}

impl<'a> MarketDataStatusBuilder<'a> {
    fn new(
        orderbook: Option<&'a OrderBookSnapshot>,
        trade: Option<&'a MarketTrade>,
        needs_resync: bool,
    ) -> Self {
        Self {
            orderbook,
            trade,
            needs_resync,
        }
    }

    fn build(self, stale_after_ms: u64) -> domain::MarketDataStatus {
        let now = now_utc();
        let last_orderbook_update_at = self.orderbook.map(|snapshot| snapshot.observed_at);
        let last_trade_update_at = self.trade.map(|trade| trade.event_time);
        let orderbook_stale = is_stale(last_orderbook_update_at, now, stale_after_ms);
        let trade_stale = self.trade.is_some() && is_stale(last_trade_update_at, now, stale_after_ms * 2);

        domain::MarketDataStatus {
            is_stale: self.needs_resync || orderbook_stale || trade_stale,
            needs_resync: self.needs_resync,
            last_orderbook_update_at,
            last_trade_update_at,
        }
    }
}

fn is_stale(last_update_at: Option<Timestamp>, now: Timestamp, stale_after_ms: u64) -> bool {
    let Some(last_update_at) = last_update_at else {
        return true;
    };
    (now - last_update_at).whole_milliseconds() > stale_after_ms as i128
}
