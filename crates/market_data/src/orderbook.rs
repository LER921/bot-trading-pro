use anyhow::{bail, Result};
use common::{Decimal, Timestamp};
use domain::{BestBidAsk, OrderBookDelta, OrderBookSnapshot, PriceLevel, Symbol};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct OrderBookCache {
    symbol: Symbol,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_update_id: u64,
    exchange_time: Option<Timestamp>,
    observed_at: Option<Timestamp>,
    needs_resync: bool,
}

impl OrderBookCache {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            exchange_time: None,
            observed_at: None,
            needs_resync: true,
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: OrderBookSnapshot) {
        self.symbol = snapshot.symbol;
        self.bids = snapshot
            .bids
            .into_iter()
            .map(|level| (level.price, level.quantity))
            .collect();
        self.asks = snapshot
            .asks
            .into_iter()
            .map(|level| (level.price, level.quantity))
            .collect();
        self.last_update_id = snapshot.last_update_id;
        self.exchange_time = snapshot.exchange_time;
        self.observed_at = Some(snapshot.observed_at);
        self.needs_resync = false;
    }

    pub fn apply_delta(&mut self, delta: OrderBookDelta) -> Result<()> {
        if self.needs_resync {
            bail!("order book for {} needs resync before applying deltas", self.symbol);
        }

        if delta.final_update_id <= self.last_update_id {
            return Ok(());
        }

        if delta.first_update_id > self.last_update_id.saturating_add(1) {
            self.needs_resync = true;
            bail!(
                "missed depth updates for {}: {} > {} + 1",
                self.symbol,
                delta.first_update_id,
                self.last_update_id
            );
        }

        apply_levels(&mut self.bids, delta.bids);
        apply_levels(&mut self.asks, delta.asks);
        self.last_update_id = delta.final_update_id;
        self.exchange_time = Some(delta.exchange_time);
        self.observed_at = Some(delta.received_at);
        Ok(())
    }

    pub fn needs_resync(&self) -> bool {
        self.needs_resync
    }

    pub fn snapshot(&self, depth: usize) -> Option<OrderBookSnapshot> {
        let observed_at = self.observed_at?;
        Some(OrderBookSnapshot {
            symbol: self.symbol,
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(price, quantity)| PriceLevel {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(price, quantity)| PriceLevel {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
            last_update_id: self.last_update_id,
            exchange_time: self.exchange_time,
            observed_at,
        })
    }

    pub fn best_bid_ask(&self) -> Option<BestBidAsk> {
        let observed_at = self.observed_at?;
        let (bid_price, bid_quantity) = self.bids.iter().next_back()?;
        let (ask_price, ask_quantity) = self.asks.iter().next()?;

        Some(BestBidAsk {
            symbol: self.symbol,
            bid_price: *bid_price,
            bid_quantity: *bid_quantity,
            ask_price: *ask_price,
            ask_quantity: *ask_quantity,
            observed_at,
        })
    }
}

fn apply_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: Vec<PriceLevel>) {
    for level in levels {
        if level.quantity.is_zero() {
            side.remove(&level.price);
        } else {
            side.insert(level.price, level.quantity);
        }
    }
}
