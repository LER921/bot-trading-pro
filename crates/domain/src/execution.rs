use crate::{LiquiditySide, Side, Symbol, TimeInForce};
use common::{Decimal, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Limit,
    Market,
    LimitMaker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

impl OrderStatus {
    pub const fn is_open(self) -> bool {
        matches!(self, Self::New | Self::PartiallyFilled)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub client_order_id: String,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub time_in_force: Option<TimeInForce>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub source_intent_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrder {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Option<Decimal>,
    pub original_quantity: Decimal,
    pub executed_quantity: Decimal,
    pub status: OrderStatus,
    pub reduce_only: bool,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub symbol: Symbol,
    pub status: OrderStatus,
    pub filled_quantity: Decimal,
    pub average_fill_price: Option<Decimal>,
    pub message: Option<String>,
    pub event_time: Timestamp,
}

impl ExecutionReport {
    pub fn to_open_order(&self, request: &OrderRequest) -> Option<OpenOrder> {
        self.status.is_open().then(|| OpenOrder {
            client_order_id: self.client_order_id.clone(),
            exchange_order_id: self.exchange_order_id.clone(),
            symbol: self.symbol,
            side: request.side,
            price: request.price,
            original_quantity: request.quantity,
            executed_quantity: self.filled_quantity,
            status: self.status,
            reduce_only: request.reduce_only,
            updated_at: self.event_time,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillEvent {
    pub trade_id: String,
    pub order_id: String,
    pub symbol: Symbol,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_asset: String,
    pub liquidity_side: LiquiditySide,
    pub event_time: Timestamp,
}
