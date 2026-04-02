use anyhow::Result;
use async_trait::async_trait;
use common::{Decimal, now_utc};
use domain::{FeatureSnapshot, MarketSnapshot, Symbol};

#[async_trait]
pub trait FeatureEngine: Send + Sync {
    async fn compute(&self, snapshot: &MarketSnapshot) -> Result<FeatureSnapshot>;
}

#[derive(Debug, Default, Clone)]
pub struct SimpleFeatureEngine;

#[async_trait]
impl FeatureEngine for SimpleFeatureEngine {
    async fn compute(&self, snapshot: &MarketSnapshot) -> Result<FeatureSnapshot> {
        let (microprice, imbalance, spread_bps) = compute_microstructure(snapshot);

        Ok(FeatureSnapshot {
            symbol: snapshot.symbol,
            microprice,
            orderbook_imbalance: imbalance,
            realized_volatility_bps: Decimal::ZERO,
            vwap_distance_bps: Decimal::ZERO,
            spread_bps,
            spread_zscore: Decimal::ZERO,
            tape_speed: Decimal::ZERO,
            local_momentum_bps: Decimal::ZERO,
            toxicity_score: Decimal::ZERO,
            computed_at: now_utc(),
        })
    }
}

fn compute_microstructure(snapshot: &MarketSnapshot) -> (Option<Decimal>, Option<Decimal>, Decimal) {
    let best = match &snapshot.best_bid_ask {
        Some(best) => best,
        None => return (None, None, Decimal::ZERO),
    };

    let total_qty = best.bid_quantity + best.ask_quantity;
    let imbalance = if total_qty.is_zero() {
        None
    } else {
        Some((best.bid_quantity - best.ask_quantity) / total_qty)
    };

    let microprice = if total_qty.is_zero() {
        None
    } else {
        Some(
            ((best.ask_price * best.bid_quantity) + (best.bid_price * best.ask_quantity))
                / total_qty,
        )
    };

    let mid = (best.bid_price + best.ask_price) / Decimal::from(2u32);
    let spread_bps = if mid.is_zero() {
        Decimal::ZERO
    } else {
        ((best.ask_price - best.bid_price) / mid) * Decimal::from(10_000u32)
    };

    (microprice, imbalance, spread_bps)
}

#[allow(dead_code)]
fn _symbol_placeholder(_symbol: Symbol) {}
