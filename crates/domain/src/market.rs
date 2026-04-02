use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Symbol {
    #[serde(rename = "BTCUSDC")]
    BtcUsdc,
    #[serde(rename = "ETHUSDC")]
    EthUsdc,
}

impl Symbol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BtcUsdc => "BTCUSDC",
            Self::EthUsdc => "ETHUSDC",
        }
    }

    pub const fn all() -> &'static [Self] {
        &[Self::BtcUsdc, Self::EthUsdc]
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for Symbol {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "BTCUSDC" | "BTC/USDC" => Ok(Self::BtcUsdc),
            "ETHUSDC" | "ETH/USDC" => Ok(Self::EthUsdc),
            other => Err(format!("unsupported symbol: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiquiditySide {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BestBidAsk {
    pub symbol: Symbol,
    pub bid_price: Decimal,
    pub bid_quantity: Decimal,
    pub ask_price: Decimal,
    pub ask_quantity: Decimal,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: Symbol,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub last_update_id: u64,
    pub exchange_time: Option<Timestamp>,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookDelta {
    pub symbol: Symbol,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_time: Timestamp,
    pub received_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketTrade {
    pub symbol: Symbol,
    pub trade_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub aggressor_side: Side,
    pub event_time: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataStatus {
    pub is_stale: bool,
    pub needs_resync: bool,
    pub last_orderbook_update_at: Option<Timestamp>,
    pub last_trade_update_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub symbol: Symbol,
    pub best_bid_ask: Option<BestBidAsk>,
    pub orderbook: Option<OrderBookSnapshot>,
    pub last_trade: Option<MarketTrade>,
    pub status: MarketDataStatus,
}
