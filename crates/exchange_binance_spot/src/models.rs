use common::{Decimal, Timestamp};
use domain::{
    AccountSnapshot, Balance, ExecutionReport, FillEvent, OpenOrder, OrderBookDelta,
    OrderBookSnapshot, OrderRequest,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamKind {
    MarketWs,
    UserWs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamLifecycle {
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStatus {
    pub kind: StreamKind,
    pub lifecycle: StreamLifecycle,
    pub reconnect_count: u64,
    pub observed_at: Timestamp,
    pub detail: String,
}

impl StreamStatus {
    pub fn connected(kind: StreamKind, reconnect_count: u64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            lifecycle: StreamLifecycle::Connected,
            reconnect_count,
            observed_at: common::now_utc(),
            detail: detail.into(),
        }
    }

    pub fn reconnecting(kind: StreamKind, reconnect_count: u64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            lifecycle: StreamLifecycle::Reconnecting,
            reconnect_count,
            observed_at: common::now_utc(),
            detail: detail.into(),
        }
    }

    pub fn disconnected(kind: StreamKind, reconnect_count: u64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            lifecycle: StreamLifecycle::Disconnected,
            reconnect_count,
            observed_at: common::now_utc(),
            detail: detail.into(),
        }
    }
}

#[derive(Debug)]
pub struct MarketStreamHandle {
    pub events: mpsc::Receiver<BinanceMarketEvent>,
    pub status: mpsc::Receiver<StreamStatus>,
    pub shutdown: watch::Sender<bool>,
}

#[derive(Debug)]
pub struct UserStreamHandle {
    pub events: mpsc::Receiver<BinanceUserStreamEvent>,
    pub status: mpsc::Receiver<StreamStatus>,
    pub shutdown: watch::Sender<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinanceMarketEvent {
    OrderBookSnapshot(OrderBookSnapshot),
    OrderBookDelta(OrderBookDelta),
    Trade(domain::MarketTrade),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceFillRecoveryRequest {
    pub symbol: domain::Symbol,
    pub trade_id: String,
    pub order_id: String,
    pub fee_asset: String,
    pub event_time: Timestamp,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceExecutionEvent {
    pub report: domain::ExecutionReport,
    pub side: domain::Side,
    pub order_type: domain::OrderType,
    pub time_in_force: Option<domain::TimeInForce>,
    pub original_quantity: Decimal,
    pub price: Option<Decimal>,
    pub cumulative_filled_quantity: Decimal,
    pub cumulative_quote_quantity: Decimal,
    pub last_executed_quantity: Decimal,
    pub last_executed_price: Option<Decimal>,
    pub reject_reason: Option<String>,
    pub is_working: bool,
    pub is_maker: bool,
    pub order_created_at: Option<Timestamp>,
    pub transaction_time: Timestamp,
    pub fill: Option<FillEvent>,
    pub fill_recovery: Option<BinanceFillRecoveryRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinanceUserStreamEvent {
    AccountSnapshot(AccountSnapshot),
    AccountPosition {
        balances: Vec<Balance>,
        updated_at: Timestamp,
    },
    BalanceDelta {
        asset: String,
        delta: Decimal,
        cleared_at: Timestamp,
        event_time: Timestamp,
    },
    Execution(BinanceExecutionEvent),
    EventStreamTerminated {
        event_time: Timestamp,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceBootstrapState {
    pub account: AccountSnapshot,
    pub open_orders: Vec<OpenOrder>,
    pub fills: Vec<FillEvent>,
    pub fetched_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreparedOrder {
    Ready(OrderRequest),
    Rejected(ExecutionReport),
}
