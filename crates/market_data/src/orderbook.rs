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
    last_event_latency_ms: Option<i64>,
    book_crossed: bool,
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
            last_event_latency_ms: None,
            book_crossed: false,
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
        self.last_event_latency_ms = snapshot
            .exchange_time
            .map(|exchange_time| (snapshot.observed_at - exchange_time).whole_milliseconds() as i64);
        self.book_crossed = false;
        self.needs_resync = false;

        if self.validate_book().is_err() {
            self.book_crossed = true;
            self.needs_resync = true;
        }
    }

    pub fn apply_delta(&mut self, delta: OrderBookDelta) -> Result<()> {
        if self.needs_resync {
            bail!("order book for {} needs resync before applying deltas", self.symbol);
        }

        if delta.final_update_id <= self.last_update_id {
            return Ok(());
        }

        let next_expected = self.last_update_id.saturating_add(1);
        if delta.first_update_id > next_expected || delta.final_update_id < next_expected {
            self.needs_resync = true;
            bail!(
                "depth sequence gap for {}: expected {}, got [{}..={}]",
                self.symbol,
                next_expected,
                delta.first_update_id,
                delta.final_update_id
            );
        }

        apply_levels(&mut self.bids, delta.bids);
        apply_levels(&mut self.asks, delta.asks);
        self.last_update_id = delta.final_update_id;
        self.exchange_time = Some(delta.exchange_time);
        self.observed_at = Some(delta.received_at);
        self.last_event_latency_ms =
            Some((delta.received_at - delta.exchange_time).whole_milliseconds() as i64);

        self.validate_book()?;
        Ok(())
    }

    pub fn needs_resync(&self) -> bool {
        self.needs_resync
    }

    pub fn last_event_latency_ms(&self) -> Option<i64> {
        self.last_event_latency_ms
    }

    pub fn book_crossed(&self) -> bool {
        self.book_crossed
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

    fn validate_book(&mut self) -> Result<()> {
        let Some(best) = self.best_bid_ask() else {
            self.needs_resync = true;
            bail!("order book for {} has no best bid/ask", self.symbol);
        };

        if best.bid_price >= best.ask_price {
            self.book_crossed = true;
            self.needs_resync = true;
            bail!(
                "order book crossed for {}: bid {} >= ask {}",
                self.symbol,
                best.bid_price,
                best.ask_price
            );
        }

        self.book_crossed = false;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::now_utc;

    fn snapshot() -> OrderBookSnapshot {
        OrderBookSnapshot {
            symbol: Symbol::BtcUsdc,
            bids: vec![PriceLevel {
                price: Decimal::from(60_000u32),
                quantity: Decimal::from_str_exact("0.5").unwrap(),
            }],
            asks: vec![PriceLevel {
                price: Decimal::from(60_010u32),
                quantity: Decimal::from_str_exact("0.4").unwrap(),
            }],
            last_update_id: 10,
            exchange_time: Some(now_utc()),
            observed_at: now_utc(),
        }
    }

    #[test]
    fn marks_book_for_resync_on_gap() {
        let mut cache = OrderBookCache::new(Symbol::BtcUsdc);
        cache.apply_snapshot(snapshot());

        let error = cache
            .apply_delta(OrderBookDelta {
                symbol: Symbol::BtcUsdc,
                first_update_id: 15,
                final_update_id: 16,
                bids: Vec::new(),
                asks: Vec::new(),
                exchange_time: now_utc(),
                received_at: now_utc(),
            })
            .expect_err("gap should require resync");

        assert!(error.to_string().contains("depth sequence gap"));
        assert!(cache.needs_resync());
    }
}
