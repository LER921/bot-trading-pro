use crate::{
    BinanceBootstrapState, BinanceExecutionEvent, BinanceMarketEvent, BinanceUserStreamEvent,
    MarketStreamHandle, StreamKind, StreamLifecycle, StreamStatus, UserStreamHandle,
};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use common::{now_utc, Decimal, Timestamp};
use domain::{
    AccountSnapshot, Balance, FillEvent, LiquiditySide, MarketTrade, OpenOrder, OrderBookDelta,
    OrderBookSnapshot, OrderRequest, OrderStatus, OrderType, PriceLevel, Side, Symbol,
    TimeInForce,
};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Client as HttpClient;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, RwLock};
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;
use url::Url;

type HmacSha256 = Hmac<Sha256>;

const STREAM_CHANNEL_CAPACITY: usize = 2048;
const USER_STREAM_RECV_WINDOW_MS: u64 = 5_000;

#[async_trait]
pub trait BinanceSpotGateway: Send + Sync {
    async fn sync_clock(&self) -> Result<Timestamp>;
    async fn ping_rest(&self) -> Result<()>;
    async fn fetch_account_snapshot(&self) -> Result<AccountSnapshot>;
    async fn fetch_open_orders(&self, symbol: Symbol) -> Result<Vec<OpenOrder>>;
    async fn fetch_recent_fills(&self, symbol: Symbol, limit: usize) -> Result<Vec<FillEvent>>;
    async fn fetch_orderbook_snapshot(&self, symbol: Symbol, depth: usize) -> Result<OrderBookSnapshot>;
    async fn fetch_recent_trades(&self, symbol: Symbol, limit: usize) -> Result<Vec<MarketTrade>>;
    async fn fetch_bootstrap_state(&self, symbols: &[Symbol]) -> Result<BinanceBootstrapState>;
    async fn poll_market_events(&self, symbols: &[Symbol]) -> Result<Vec<BinanceMarketEvent>>;
    async fn poll_account_events(&self, symbols: &[Symbol]) -> Result<Vec<BinanceUserStreamEvent>>;
    async fn place_order(&self, request: OrderRequest) -> Result<domain::ExecutionReport>;
    async fn cancel_all_orders(&self, symbol: Symbol) -> Result<()>;
    async fn open_market_stream(&self, symbols: &[Symbol]) -> Result<MarketStreamHandle>;
    async fn open_user_stream(&self) -> Result<UserStreamHandle>;
}

#[derive(Debug, Clone)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub api_secret: String,
}

impl BinanceCredentials {
    pub fn from_env(api_key_env: &str, api_secret_env: &str) -> Result<Self> {
        let api_key = env::var(api_key_env)
            .with_context(|| format!("missing environment variable {api_key_env}"))?;
        let api_secret = env::var(api_secret_env)
            .with_context(|| format!("missing environment variable {api_secret_env}"))?;

        Ok(Self {
            api_key,
            api_secret,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BinanceSpotClient {
    pub rest_base_url: Url,
    pub market_ws_url: Url,
    pub user_ws_url: Url,
    pub recv_window_ms: u64,
    credentials: BinanceCredentials,
    http_client: HttpClient,
}

impl BinanceSpotClient {
    pub fn new(
        rest_base_url: impl AsRef<str>,
        market_ws_url: impl AsRef<str>,
        user_ws_url: impl AsRef<str>,
        recv_window_ms: u64,
        credentials: BinanceCredentials,
    ) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-MBX-APIKEY",
            HeaderValue::from_str(&credentials.api_key).context("invalid Binance API key header")?,
        );

        Ok(Self {
            rest_base_url: Url::parse(rest_base_url.as_ref())?,
            market_ws_url: Url::parse(market_ws_url.as_ref())?,
            user_ws_url: Url::parse(user_ws_url.as_ref())?,
            recv_window_ms,
            credentials,
            http_client: HttpClient::builder().default_headers(headers).build()?,
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url> {
        self.rest_base_url
            .join(path)
            .with_context(|| format!("failed to build Binance endpoint for path {path}"))
    }

    fn sign_rest_query(&self, query: &str) -> Result<String> {
        sign_hmac_hex(&self.credentials.api_secret, query)
    }

    fn websocket_user_stream_request(&self) -> Result<String> {
        let timestamp = unix_timestamp_ms_u64(now_utc());
        let params = vec![
            ("apiKey", self.credentials.api_key.clone()),
            ("recvWindow", USER_STREAM_RECV_WINDOW_MS.to_string()),
            ("timestamp", timestamp.to_string()),
        ];
        let signature_payload = signature_payload(&params);
        let signature = sign_hmac_hex(&self.credentials.api_secret, &signature_payload)?;
        let api_key = self.credentials.api_key.clone();

        Ok(json!({
            "id": common::ids::new_request_id(),
            "method": "userDataStream.subscribe.signature",
            "params": {
                "apiKey": api_key,
                "recvWindow": USER_STREAM_RECV_WINDOW_MS,
                "timestamp": timestamp,
                "signature": signature
            }
        })
        .to_string())
    }

    fn combined_market_stream_url(&self, symbols: &[Symbol]) -> String {
        let streams = symbols
            .iter()
            .flat_map(|symbol| {
                let name = symbol.as_str().to_ascii_lowercase();
                [format!("{name}@depth@100ms"), format!("{name}@trade")]
            })
            .collect::<Vec<_>>()
            .join("/");

        let mut url = self.market_ws_url.clone();
        url.set_query(Some(&format!("streams={streams}")));
        url.to_string()
    }

    async fn get_public<T>(&self, path: &str, query: &[(&str, String)]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let url = self.endpoint(path)?;
        let response = self
            .http_client
            .get(url)
            .query(query)
            .send()
            .await?
            .error_for_status()?;

        Ok(response.json::<T>().await?)
    }

    async fn signed_get<T>(&self, path: &str, extra_query: &[(&str, String)]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let query = signed_query(self.recv_window_ms, extra_query);
        let encoded = encode_query(&query);
        let signature = self.sign_rest_query(&encoded)?;
        let mut url = self.endpoint(path)?;
        url.set_query(Some(&format!("{encoded}&signature={signature}")));

        let response = self.http_client.get(url).send().await?.error_for_status()?;
        Ok(response.json::<T>().await?)
    }

    async fn signed_post<T>(&self, path: &str, extra_query: &[(&str, String)]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let query = signed_query(self.recv_window_ms, extra_query);
        let encoded = encode_query(&query);
        let signature = self.sign_rest_query(&encoded)?;
        let payload = format!("{encoded}&signature={signature}");

        let response = self
            .http_client
            .post(self.endpoint(path)?)
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(payload)
            .send()
            .await?
            .error_for_status()?;

        Ok(response.json::<T>().await?)
    }

    async fn signed_delete_empty(&self, path: &str, extra_query: &[(&str, String)]) -> Result<()> {
        let query = signed_query(self.recv_window_ms, extra_query);
        let encoded = encode_query(&query);
        let signature = self.sign_rest_query(&encoded)?;
        let payload = format!("{encoded}&signature={signature}");

        self.http_client
            .delete(self.endpoint(path)?)
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(payload)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

#[async_trait]
impl BinanceSpotGateway for BinanceSpotClient {
    async fn sync_clock(&self) -> Result<Timestamp> {
        let response: BinanceServerTimeResponse = self.get_public("/api/v3/time", &[]).await?;
        millis_timestamp(response.server_time)
    }

    async fn ping_rest(&self) -> Result<()> {
        let _: Value = self.get_public("/api/v3/ping", &[]).await?;
        Ok(())
    }

    async fn fetch_account_snapshot(&self) -> Result<AccountSnapshot> {
        let response: BinanceAccountResponse = self.signed_get("/api/v3/account", &[]).await?;
        Ok(AccountSnapshot {
            balances: response
                .balances
                .into_iter()
                .map(|balance| -> Result<Balance> {
                    Ok(Balance {
                        asset: balance.asset,
                        free: parse_decimal(&balance.free)?,
                        locked: parse_decimal(&balance.locked)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            updated_at: millis_timestamp(response.update_time)?,
        })
    }

    async fn fetch_open_orders(&self, symbol: Symbol) -> Result<Vec<OpenOrder>> {
        let response: Vec<BinanceOpenOrderResponse> = self
            .signed_get(
                "/api/v3/openOrders",
                &[("symbol", symbol.as_str().to_string())],
            )
            .await?;

        response
            .into_iter()
            .map(parse_open_order_response)
            .collect::<Result<Vec<_>>>()
    }

    async fn fetch_recent_fills(&self, symbol: Symbol, limit: usize) -> Result<Vec<FillEvent>> {
        let response: Vec<BinanceMyTradeResponse> = self
            .signed_get(
                "/api/v3/myTrades",
                &[
                    ("symbol", symbol.as_str().to_string()),
                    ("limit", limit.min(1_000).to_string()),
                ],
            )
            .await?;

        response
            .into_iter()
            .map(|trade| parse_my_trade_response(symbol, trade))
            .collect::<Result<Vec<_>>>()
    }

    async fn fetch_orderbook_snapshot(&self, symbol: Symbol, depth: usize) -> Result<OrderBookSnapshot> {
        let response: BinanceDepthSnapshotResponse = self
            .get_public(
                "/api/v3/depth",
                &[
                    ("symbol", symbol.as_str().to_string()),
                    ("limit", depth.min(1_000).to_string()),
                ],
            )
            .await?;

        Ok(OrderBookSnapshot {
            symbol,
            bids: parse_price_levels(response.bids)?,
            asks: parse_price_levels(response.asks)?,
            last_update_id: response.last_update_id,
            exchange_time: None,
            observed_at: now_utc(),
        })
    }

    async fn fetch_recent_trades(&self, symbol: Symbol, limit: usize) -> Result<Vec<MarketTrade>> {
        let response: Vec<BinanceTradeResponse> = self
            .get_public(
                "/api/v3/trades",
                &[
                    ("symbol", symbol.as_str().to_string()),
                    ("limit", limit.min(1_000).to_string()),
                ],
            )
            .await?;

        response
            .into_iter()
            .map(|trade| parse_market_trade_response(symbol, trade))
            .collect::<Result<Vec<_>>>()
    }

    async fn fetch_bootstrap_state(&self, symbols: &[Symbol]) -> Result<BinanceBootstrapState> {
        let account = self.fetch_account_snapshot().await?;
        let mut open_orders = Vec::new();
        let mut fills = Vec::new();

        for &symbol in symbols {
            open_orders.extend(self.fetch_open_orders(symbol).await?);
            fills.extend(self.fetch_recent_fills(symbol, 20).await?);
        }

        fills.sort_by_key(|fill| fill.event_time);
        Ok(BinanceBootstrapState {
            account,
            open_orders,
            fills,
            fetched_at: now_utc(),
        })
    }

    async fn poll_market_events(&self, symbols: &[Symbol]) -> Result<Vec<BinanceMarketEvent>> {
        let mut events = Vec::new();
        for &symbol in symbols {
            events.push(BinanceMarketEvent::OrderBookSnapshot(
                self.fetch_orderbook_snapshot(symbol, 50).await?,
            ));

            if let Some(trade) = self.fetch_recent_trades(symbol, 1).await?.into_iter().last() {
                events.push(BinanceMarketEvent::Trade(trade));
            }
        }
        Ok(events)
    }

    async fn poll_account_events(&self, _symbols: &[Symbol]) -> Result<Vec<BinanceUserStreamEvent>> {
        Ok(vec![BinanceUserStreamEvent::AccountSnapshot(
            self.fetch_account_snapshot().await?,
        )])
    }

    async fn place_order(&self, request: OrderRequest) -> Result<domain::ExecutionReport> {
        let mut params = vec![
            ("symbol", request.symbol.as_str().to_string()),
            ("side", serialize_side(request.side).to_string()),
            ("type", serialize_order_type(request.order_type).to_string()),
            ("quantity", request.quantity.to_string()),
            ("newClientOrderId", request.client_order_id.clone()),
            ("newOrderRespType", "RESULT".to_string()),
        ];

        if let Some(price) = request.price {
            params.push(("price", price.to_string()));
        }

        if let Some(time_in_force) = request.time_in_force {
            params.push(("timeInForce", serialize_time_in_force(time_in_force).to_string()));
        }

        let response: BinanceOrderResponse = self.signed_post("/api/v3/order", &params).await?;
        parse_order_response(request.symbol, response)
    }

    async fn cancel_all_orders(&self, symbol: Symbol) -> Result<()> {
        self.signed_delete_empty(
            "/api/v3/openOrders",
            &[("symbol", symbol.as_str().to_string())],
        )
        .await
    }

    async fn open_market_stream(&self, symbols: &[Symbol]) -> Result<MarketStreamHandle> {
        let (event_tx, event_rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let (status_tx, status_rx) = mpsc::channel(128);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        tokio::spawn(run_market_stream_task(
            self.clone(),
            symbols.to_vec(),
            event_tx,
            status_tx,
            shutdown_rx,
        ));

        Ok(MarketStreamHandle {
            events: event_rx,
            status: status_rx,
            shutdown: shutdown_tx,
        })
    }

    async fn open_user_stream(&self) -> Result<UserStreamHandle> {
        let (event_tx, event_rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let (status_tx, status_rx) = mpsc::channel(128);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        tokio::spawn(run_user_stream_task(
            self.clone(),
            event_tx,
            status_tx,
            shutdown_rx,
        ));

        Ok(UserStreamHandle {
            events: event_rx,
            status: status_rx,
            shutdown: shutdown_tx,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BinanceServerTimeResponse {
    #[serde(rename = "serverTime")]
    server_time: i64,
}

#[derive(Debug, Deserialize)]
struct BinanceBalanceResponse {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
struct BinanceAccountResponse {
    balances: Vec<BinanceBalanceResponse>,
    #[serde(rename = "updateTime")]
    update_time: i64,
}

#[derive(Debug, Deserialize)]
struct BinanceOpenOrderResponse {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: u64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    price: String,
    #[serde(rename = "origQty")]
    original_quantity: String,
    #[serde(rename = "executedQty")]
    executed_quantity: String,
    status: String,
    side: String,
    time: u64,
}

#[derive(Debug, Deserialize)]
struct BinanceMyTradeResponse {
    id: u64,
    #[serde(rename = "orderId")]
    order_id: u64,
    price: String,
    qty: String,
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
    time: u64,
    #[serde(rename = "isBuyer")]
    is_buyer: bool,
    #[serde(rename = "isMaker")]
    is_maker: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceDepthSnapshotResponse {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct BinanceTradeResponse {
    id: u64,
    price: String,
    qty: String,
    time: u64,
    #[serde(rename = "isBuyerMaker")]
    is_buyer_maker: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceOrderResponse {
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: u64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    status: String,
    #[serde(rename = "executedQty")]
    executed_quantity: String,
    #[serde(rename = "cummulativeQuoteQty")]
    cumulative_quote_quantity: String,
    #[serde(rename = "transactTime")]
    transact_time: u64,
}

async fn run_market_stream_task(
    client: BinanceSpotClient,
    symbols: Vec<Symbol>,
    event_tx: mpsc::Sender<BinanceMarketEvent>,
    status_tx: mpsc::Sender<StreamStatus>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut reconnect_count = 0u64;
    let mut backoff = Duration::from_secs(1);

    loop {
        if *shutdown_rx.borrow() {
            let _ = status_tx
                .send(StreamStatus::disconnected(
                    StreamKind::MarketWs,
                    reconnect_count,
                    "market stream shutdown requested",
                ))
                .await;
            return;
        }

        let _ = status_tx
            .send(StreamStatus {
                kind: StreamKind::MarketWs,
                lifecycle: if reconnect_count == 0 {
                    StreamLifecycle::Connecting
                } else {
                    StreamLifecycle::Reconnecting
                },
                reconnect_count,
                observed_at: now_utc(),
                detail: "connecting market websocket".to_string(),
            })
            .await;

        let connect_url = client.combined_market_stream_url(&symbols);
        match connect_async(connect_url.as_str()).await {
            Ok((stream, _)) => {
                let _ = status_tx
                    .send(StreamStatus::connected(
                        StreamKind::MarketWs,
                        reconnect_count,
                        "market websocket connected",
                    ))
                    .await;

                let (mut writer, mut reader) = stream.split();
                let mut ping = interval(Duration::from_secs(15));
                backoff = Duration::from_secs(1);

                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_ok() && *shutdown_rx.borrow() {
                                let _ = writer.send(Message::Close(None)).await;
                                let _ = status_tx.send(StreamStatus::disconnected(
                                    StreamKind::MarketWs,
                                    reconnect_count,
                                    "market websocket closed by runtime",
                                )).await;
                                return;
                            }
                        }
                        _ = ping.tick() => {
                            if let Err(error) = writer.send(Message::Ping(Vec::new())).await {
                                warn!(?error, "market websocket ping failed");
                                break;
                            }
                        }
                        maybe_message = reader.next() => {
                            match maybe_message {
                                Some(Ok(Message::Text(text))) => match parse_market_stream_message(&text) {
                                    Ok(Some(event)) => {
                                        if event_tx.send(event).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => warn!(?error, "failed to parse market websocket message"),
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(error) = writer.send(Message::Pong(payload)).await {
                                        warn!(?error, "failed to answer market ping");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {}
                                Some(Ok(Message::Binary(_))) => {}
                                Some(Ok(Message::Frame(_))) => {}
                                Some(Ok(Message::Close(frame))) => {
                                    warn!(?frame, "market websocket closed by peer");
                                    break;
                                }
                                Some(Err(error)) => {
                                    warn!(?error, "market websocket read failed");
                                    break;
                                }
                                None => {
                                    warn!("market websocket stream ended");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(error) => warn!(?error, "market websocket connect failed"),
        }

        reconnect_count = reconnect_count.saturating_add(1);
        let _ = status_tx
            .send(StreamStatus::reconnecting(
                StreamKind::MarketWs,
                reconnect_count,
                "market websocket reconnect scheduled",
            ))
            .await;
        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn run_user_stream_task(
    client: BinanceSpotClient,
    event_tx: mpsc::Sender<BinanceUserStreamEvent>,
    status_tx: mpsc::Sender<StreamStatus>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut reconnect_count = 0u64;
    let mut backoff = Duration::from_secs(1);

    loop {
        if *shutdown_rx.borrow() {
            let _ = status_tx
                .send(StreamStatus::disconnected(
                    StreamKind::UserWs,
                    reconnect_count,
                    "user stream shutdown requested",
                ))
                .await;
            return;
        }

        let _ = status_tx
            .send(StreamStatus {
                kind: StreamKind::UserWs,
                lifecycle: if reconnect_count == 0 {
                    StreamLifecycle::Connecting
                } else {
                    StreamLifecycle::Reconnecting
                },
                reconnect_count,
                observed_at: now_utc(),
                detail: "connecting user websocket".to_string(),
            })
            .await;

        match connect_async(client.user_ws_url.as_str()).await {
            Ok((stream, _)) => {
                let (mut writer, mut reader) = stream.split();
                if let Err(error) = writer
                    .send(Message::Text(
                        client.websocket_user_stream_request().unwrap_or_default().into(),
                    ))
                    .await
                {
                    warn!(?error, "failed to subscribe user websocket");
                } else {
                    let _ = status_tx
                        .send(StreamStatus::connected(
                            StreamKind::UserWs,
                            reconnect_count,
                            "user websocket connected and subscription requested",
                        ))
                        .await;
                }

                let mut ping = interval(Duration::from_secs(15));
                backoff = Duration::from_secs(1);

                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_ok() && *shutdown_rx.borrow() {
                                let _ = writer.send(Message::Close(None)).await;
                                let _ = status_tx.send(StreamStatus::disconnected(
                                    StreamKind::UserWs,
                                    reconnect_count,
                                    "user websocket closed by runtime",
                                )).await;
                                return;
                            }
                        }
                        _ = ping.tick() => {
                            if let Err(error) = writer.send(Message::Ping(Vec::new())).await {
                                warn!(?error, "user websocket ping failed");
                                break;
                            }
                        }
                        maybe_message = reader.next() => {
                            match maybe_message {
                                Some(Ok(Message::Text(text))) => match parse_user_stream_message(&text) {
                                    Ok(Some(event)) => {
                                        if event_tx.send(event).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(error) => warn!(?error, "failed to parse user websocket message"),
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(error) = writer.send(Message::Pong(payload)).await {
                                        warn!(?error, "failed to answer user ping");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {}
                                Some(Ok(Message::Binary(_))) => {}
                                Some(Ok(Message::Frame(_))) => {}
                                Some(Ok(Message::Close(frame))) => {
                                    warn!(?frame, "user websocket closed by peer");
                                    break;
                                }
                                Some(Err(error)) => {
                                    warn!(?error, "user websocket read failed");
                                    break;
                                }
                                None => {
                                    warn!("user websocket stream ended");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(error) => warn!(?error, "user websocket connect failed"),
        }

        reconnect_count = reconnect_count.saturating_add(1);
        let _ = status_tx
            .send(StreamStatus::reconnecting(
                StreamKind::UserWs,
                reconnect_count,
                "user websocket reconnect scheduled",
            ))
            .await;
        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn parse_market_stream_message(raw: &str) -> Result<Option<BinanceMarketEvent>> {
    let payload: Value = serde_json::from_str(raw)?;
    let Some(data) = payload.get("data").or_else(|| payload.get("event")).or(Some(&payload)) else {
        return Ok(None);
    };
    let stream_name = payload
        .get("stream")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    if stream_name.contains("@depth") || data.get("e").and_then(Value::as_str) == Some("depthUpdate") {
        return parse_depth_event(data).map(Some);
    }

    if stream_name.contains("@trade") || data.get("e").and_then(Value::as_str) == Some("trade") {
        return parse_trade_event(data).map(Some);
    }

    Ok(None)
}

fn parse_user_stream_message(raw: &str) -> Result<Option<BinanceUserStreamEvent>> {
    let payload: Value = serde_json::from_str(raw)?;
    if payload.get("status").is_some() {
        return Ok(None);
    }

    let Some(event) = payload.get("event").or_else(|| payload.get("data")).or(Some(&payload)) else {
        return Ok(None);
    };

    match event_type(event)? {
        "outboundAccountPosition" => parse_account_position_event(event).map(Some),
        "balanceUpdate" | "externalLockUpdate" => parse_balance_delta_event(event).map(Some),
        "executionReport" => parse_execution_report_event(event).map(Some),
        "eventStreamTerminated" => Ok(Some(BinanceUserStreamEvent::EventStreamTerminated {
            event_time: millis_timestamp(required_i64(event, "E")?)?,
        })),
        _ => Ok(None),
    }
}

fn parse_open_order_response(response: BinanceOpenOrderResponse) -> Result<OpenOrder> {
    Ok(OpenOrder {
        client_order_id: response.client_order_id,
        exchange_order_id: Some(response.order_id.to_string()),
        symbol: Symbol::from_str(&response.symbol).map_err(|error| anyhow!(error))?,
        side: parse_side(&response.side)?,
        price: optional_decimal(&response.price)?,
        original_quantity: parse_decimal(&response.original_quantity)?,
        executed_quantity: parse_decimal(&response.executed_quantity)?,
        status: parse_order_status(&response.status)?,
        reduce_only: false,
        updated_at: millis_timestamp(response.time as i64)?,
    })
}

fn parse_my_trade_response(symbol: Symbol, trade: BinanceMyTradeResponse) -> Result<FillEvent> {
    Ok(FillEvent {
        trade_id: trade.id.to_string(),
        order_id: trade.order_id.to_string(),
        symbol,
        side: if trade.is_buyer { Side::Buy } else { Side::Sell },
        price: parse_decimal(&trade.price)?,
        quantity: parse_decimal(&trade.qty)?,
        fee: parse_decimal(&trade.commission)?,
        fee_asset: trade.commission_asset,
        liquidity_side: if trade.is_maker {
            LiquiditySide::Maker
        } else {
            LiquiditySide::Taker
        },
        event_time: millis_timestamp(trade.time as i64)?,
    })
}

fn parse_market_trade_response(symbol: Symbol, trade: BinanceTradeResponse) -> Result<MarketTrade> {
    let event_time = millis_timestamp(trade.time as i64)?;
    Ok(MarketTrade {
        symbol,
        trade_id: trade.id.to_string(),
        price: parse_decimal(&trade.price)?,
        quantity: parse_decimal(&trade.qty)?,
        aggressor_side: if trade.is_buyer_maker {
            Side::Sell
        } else {
            Side::Buy
        },
        event_time,
        received_at: now_utc(),
    })
}

fn parse_order_response(expected_symbol: Symbol, response: BinanceOrderResponse) -> Result<domain::ExecutionReport> {
    let filled_quantity = parse_decimal(&response.executed_quantity)?;
    let cumulative_quote = parse_decimal(&response.cumulative_quote_quantity)?;
    let requested_price = None::<Decimal>;
    Ok(domain::ExecutionReport {
        client_order_id: response.client_order_id,
        exchange_order_id: Some(response.order_id.to_string()),
        symbol: Symbol::from_str(&response.symbol).map_err(|error| anyhow!(error))?,
        status: parse_order_status(&response.status)?,
        filled_quantity,
        average_fill_price: average_fill_price(filled_quantity, cumulative_quote),
        fill_ratio: Decimal::ZERO,
        requested_price,
        slippage_bps: None,
        decision_latency_ms: None,
        message: Some(format!("order accepted for {}", expected_symbol)),
        event_time: millis_timestamp(response.transact_time as i64)?,
    })
}

fn parse_price_levels(levels: Vec<[String; 2]>) -> Result<Vec<PriceLevel>> {
    levels
        .into_iter()
        .map(|[price, quantity]| {
            Ok(PriceLevel {
                price: parse_decimal(&price)?,
                quantity: parse_decimal(&quantity)?,
            })
        })
        .collect()
}

fn parse_depth_event(data: &Value) -> Result<BinanceMarketEvent> {
    let symbol = parse_symbol(required_str(data, "s")?)?;
    Ok(BinanceMarketEvent::OrderBookDelta(OrderBookDelta {
        symbol,
        first_update_id: required_u64(data, "U")?,
        final_update_id: required_u64(data, "u")?,
        bids: parse_value_price_levels(required_array(data, "b")?)?,
        asks: parse_value_price_levels(required_array(data, "a")?)?,
        exchange_time: millis_timestamp(required_i64(data, "E")?)?,
        received_at: now_utc(),
    }))
}

fn parse_trade_event(data: &Value) -> Result<BinanceMarketEvent> {
    let symbol = parse_symbol(required_str(data, "s")?)?;
    let event_time = millis_timestamp(required_i64(data, "T")?)?;
    Ok(BinanceMarketEvent::Trade(MarketTrade {
        symbol,
        trade_id: required_u64(data, "t")?.to_string(),
        price: parse_decimal(required_str(data, "p")?)?,
        quantity: parse_decimal(required_str(data, "q")?)?,
        aggressor_side: if required_bool(data, "m")? {
            Side::Sell
        } else {
            Side::Buy
        },
        event_time,
        received_at: now_utc(),
    }))
}

fn parse_account_position_event(event: &Value) -> Result<BinanceUserStreamEvent> {
    Ok(BinanceUserStreamEvent::AccountPosition {
        balances: parse_balance_array(required_array(event, "B")?)?,
        updated_at: millis_timestamp(required_i64(event, "u")?)?,
    })
}

fn parse_balance_delta_event(event: &Value) -> Result<BinanceUserStreamEvent> {
    Ok(BinanceUserStreamEvent::BalanceDelta {
        asset: required_str(event, "a")?.to_string(),
        delta: parse_decimal(required_str(event, "d")?)?,
        cleared_at: millis_timestamp(required_i64(event, "T")?)?,
        event_time: millis_timestamp(required_i64(event, "E")?)?,
    })
}

fn parse_execution_report_event(event: &Value) -> Result<BinanceUserStreamEvent> {
    let symbol = parse_symbol(required_str(event, "s")?)?;
    let status = parse_order_status(required_str(event, "X")?)?;
    let cumulative_filled_quantity = parse_decimal(required_str(event, "z")?)?;
    let cumulative_quote_quantity = parse_decimal(required_str(event, "Z")?)?;
    let last_executed_quantity = parse_decimal(required_str(event, "l")?)?;
    let last_executed_price = optional_decimal(required_str(event, "L")?)?;
    let trade_id = event.get("t").and_then(Value::as_i64).unwrap_or(-1);
    let fill = if last_executed_quantity > Decimal::ZERO && trade_id >= 0 {
        Some(FillEvent {
            trade_id: trade_id.to_string(),
            order_id: required_u64(event, "i")?.to_string(),
            symbol,
            side: parse_side(required_str(event, "S")?)?,
            price: last_executed_price.unwrap_or(Decimal::ZERO),
            quantity: last_executed_quantity,
            fee: parse_decimal(required_str(event, "n")?)?,
            fee_asset: event
                .get("N")
                .and_then(Value::as_str)
                .unwrap_or("UNKNOWN")
                .to_string(),
            liquidity_side: if required_bool(event, "m")? {
                LiquiditySide::Maker
            } else {
                LiquiditySide::Taker
            },
            event_time: millis_timestamp(required_i64(event, "T")?)?,
        })
    } else {
        None
    };

    Ok(BinanceUserStreamEvent::Execution(BinanceExecutionEvent {
        report: domain::ExecutionReport {
            client_order_id: required_str(event, "c")?.to_string(),
            exchange_order_id: Some(required_u64(event, "i")?.to_string()),
            symbol,
            status,
            filled_quantity: cumulative_filled_quantity,
            average_fill_price: average_fill_price(cumulative_filled_quantity, cumulative_quote_quantity),
            fill_ratio: {
                let original_quantity = parse_decimal(required_str(event, "q")?)?;
                if original_quantity.is_zero() {
                    Decimal::ZERO
                } else {
                    cumulative_filled_quantity / original_quantity
                }
            },
            requested_price: optional_decimal(required_str(event, "p")?)?,
            slippage_bps: match (
                last_executed_price,
                optional_decimal(required_str(event, "p")?)?,
            ) {
                (Some(fill_price), Some(limit_price)) if !limit_price.is_zero() => Some(
                    ((fill_price - limit_price).abs() / limit_price) * Decimal::from(10_000u32),
                ),
                _ => None,
            },
            decision_latency_ms: event
                .get("O")
                .and_then(Value::as_i64)
                .map(|created| required_i64(event, "E").map(|event_time| event_time - created))
                .transpose()?,
            message: non_empty_string(event.get("r").and_then(Value::as_str))
                .filter(|reason| reason != "NONE"),
            event_time: millis_timestamp(required_i64(event, "E")?)?,
        },
        side: parse_side(required_str(event, "S")?)?,
        order_type: parse_order_type(required_str(event, "o")?)?,
        time_in_force: optional_time_in_force(event.get("f").and_then(Value::as_str))?,
        original_quantity: parse_decimal(required_str(event, "q")?)?,
        price: optional_decimal(required_str(event, "p")?)?,
        cumulative_filled_quantity,
        cumulative_quote_quantity,
        last_executed_quantity,
        last_executed_price,
        reject_reason: non_empty_string(event.get("r").and_then(Value::as_str))
            .filter(|reason| reason != "NONE"),
        is_working: required_bool(event, "w")?,
        is_maker: required_bool(event, "m")?,
        order_created_at: event
            .get("O")
            .and_then(Value::as_i64)
            .map(millis_timestamp)
            .transpose()?,
        transaction_time: millis_timestamp(required_i64(event, "T")?)?,
        fill,
    }))
}

fn parse_balance_array(values: &[Value]) -> Result<Vec<Balance>> {
    values
        .iter()
        .map(|balance| {
            Ok(Balance {
                asset: required_str(balance, "a")?.to_string(),
                free: parse_decimal(required_str(balance, "f")?)?,
                locked: parse_decimal(required_str(balance, "l")?)?,
            })
        })
        .collect()
}

fn parse_value_price_levels(values: &[Value]) -> Result<Vec<PriceLevel>> {
    values
        .iter()
        .map(|value| {
            let level = value
                .as_array()
                .ok_or_else(|| anyhow!("price level must be an array"))?;
            if level.len() < 2 {
                bail!("price level must have [price, quantity]");
            }
            Ok(PriceLevel {
                price: parse_decimal(level[0].as_str().ok_or_else(|| anyhow!("price must be a string"))?)?,
                quantity: parse_decimal(level[1].as_str().ok_or_else(|| anyhow!("quantity must be a string"))?)?,
            })
        })
        .collect()
}

fn parse_symbol(raw: &str) -> Result<Symbol> {
    Symbol::from_str(raw).map_err(|error| anyhow!(error))
}

fn parse_side(raw: &str) -> Result<Side> {
    match raw {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other => bail!("unsupported Binance side: {other}"),
    }
}

fn parse_order_type(raw: &str) -> Result<OrderType> {
    match raw {
        "LIMIT" => Ok(OrderType::Limit),
        "MARKET" => Ok(OrderType::Market),
        "LIMIT_MAKER" => Ok(OrderType::LimitMaker),
        other => bail!("unsupported Binance order type: {other}"),
    }
}

fn parse_order_status(raw: &str) -> Result<OrderStatus> {
    match raw {
        "NEW" => Ok(OrderStatus::New),
        "PARTIALLY_FILLED" => Ok(OrderStatus::PartiallyFilled),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" => Ok(OrderStatus::Canceled),
        "REJECTED" => Ok(OrderStatus::Rejected),
        "EXPIRED" => Ok(OrderStatus::Expired),
        other => bail!("unsupported Binance order status: {other}"),
    }
}

fn parse_time_in_force(raw: &str) -> Result<TimeInForce> {
    match raw {
        "GTC" => Ok(TimeInForce::Gtc),
        "IOC" => Ok(TimeInForce::Ioc),
        "FOK" => Ok(TimeInForce::Fok),
        other => bail!("unsupported Binance time in force: {other}"),
    }
}

fn optional_time_in_force(raw: Option<&str>) -> Result<Option<TimeInForce>> {
    match raw {
        Some("") | None => Ok(None),
        Some(raw) => parse_time_in_force(raw).map(Some),
    }
}

fn serialize_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn serialize_order_type(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Limit => "LIMIT",
        OrderType::Market => "MARKET",
        OrderType::LimitMaker => "LIMIT_MAKER",
    }
}

fn serialize_time_in_force(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::Gtc => "GTC",
        TimeInForce::Ioc => "IOC",
        TimeInForce::Fok => "FOK",
    }
}

fn signature_payload(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn signed_query(recv_window_ms: u64, extra_query: &[(&str, String)]) -> Vec<(String, String)> {
    let mut query = extra_query
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect::<Vec<_>>();
    query.push(("recvWindow".to_string(), recv_window_ms.to_string()));
    query.push((
        "timestamp".to_string(),
        unix_timestamp_ms_u64(now_utc()).to_string(),
    ));
    query
}

fn encode_query(query: &[(String, String)]) -> String {
    query
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn sign_hmac_hex(secret: &str, payload: &str) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .context("failed to initialize Binance HMAC signer")?;
    mac.update(payload.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn required_array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| anyhow!("missing or invalid array field `{key}`"))
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing or invalid string field `{key}`"))
}

fn required_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing or invalid integer field `{key}`"))
}

fn required_u64(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing or invalid unsigned integer field `{key}`"))
}

fn required_bool(value: &Value, key: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("missing or invalid bool field `{key}`"))
}

fn event_type<'a>(event: &'a Value) -> Result<&'a str> {
    event
        .get("e")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing event type"))
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    Decimal::from_str_exact(raw).with_context(|| format!("invalid decimal `{raw}`"))
}

fn optional_decimal(raw: &str) -> Result<Option<Decimal>> {
    let decimal = parse_decimal(raw)?;
    Ok((!decimal.is_zero()).then_some(decimal))
}

fn average_fill_price(filled_quantity: Decimal, quote_quantity: Decimal) -> Option<Decimal> {
    (!filled_quantity.is_zero()).then_some(quote_quantity / filled_quantity)
}

fn millis_timestamp(raw: i64) -> Result<Timestamp> {
    time::OffsetDateTime::from_unix_timestamp_nanos(raw as i128 * 1_000_000)
        .context("invalid Binance millisecond timestamp")
}

fn unix_timestamp_ms_u64(timestamp: Timestamp) -> u64 {
    common::unix_timestamp_ms(timestamp).max(0) as u64
}

fn non_empty_string(raw: Option<&str>) -> Option<String> {
    raw.filter(|value| !value.is_empty()).map(str::to_string)
}

#[derive(Debug)]
struct MockStreamSubscribers<T> {
    senders: Vec<mpsc::Sender<T>>,
}

impl<T> Default for MockStreamSubscribers<T> {
    fn default() -> Self {
        Self { senders: Vec::new() }
    }
}

#[derive(Debug)]
struct MockGatewayState {
    clock_time: Timestamp,
    account: Option<AccountSnapshot>,
    orderbooks: HashMap<Symbol, OrderBookSnapshot>,
    trades: HashMap<Symbol, Vec<MarketTrade>>,
    open_orders: HashMap<Symbol, Vec<OpenOrder>>,
    fills: HashMap<Symbol, Vec<FillEvent>>,
    placed_orders: Vec<OrderRequest>,
    market_events: VecDeque<BinanceMarketEvent>,
    account_events: VecDeque<BinanceUserStreamEvent>,
    market_status_events: VecDeque<StreamStatus>,
    user_status_events: VecDeque<StreamStatus>,
    market_streams: MockStreamSubscribers<BinanceMarketEvent>,
    user_streams: MockStreamSubscribers<BinanceUserStreamEvent>,
    market_status_streams: MockStreamSubscribers<StreamStatus>,
    user_status_streams: MockStreamSubscribers<StreamStatus>,
}

impl Default for MockGatewayState {
    fn default() -> Self {
        Self {
            clock_time: now_utc(),
            account: None,
            orderbooks: HashMap::new(),
            trades: HashMap::new(),
            open_orders: HashMap::new(),
            fills: HashMap::new(),
            placed_orders: Vec::new(),
            market_events: VecDeque::new(),
            account_events: VecDeque::new(),
            market_status_events: VecDeque::new(),
            user_status_events: VecDeque::new(),
            market_streams: MockStreamSubscribers::default(),
            user_streams: MockStreamSubscribers::default(),
            market_status_streams: MockStreamSubscribers::default(),
            user_status_streams: MockStreamSubscribers::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MockBinanceSpotGateway {
    state: Arc<RwLock<MockGatewayState>>,
}

impl MockBinanceSpotGateway {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_clock_time(&self, timestamp: Timestamp) {
        self.state.write().await.clock_time = timestamp;
    }

    pub async fn seed_account(&self, account: AccountSnapshot) {
        self.state.write().await.account = Some(account);
    }

    pub async fn seed_orderbook(&self, snapshot: OrderBookSnapshot) {
        self.state
            .write()
            .await
            .orderbooks
            .insert(snapshot.symbol, snapshot);
    }

    pub async fn seed_open_orders(&self, symbol: Symbol, orders: Vec<OpenOrder>) {
        self.state.write().await.open_orders.insert(symbol, orders);
    }

    pub async fn seed_fills(&self, symbol: Symbol, fills: Vec<FillEvent>) {
        self.state.write().await.fills.insert(symbol, fills);
    }

    pub async fn seed_trades(&self, symbol: Symbol, trades: Vec<MarketTrade>) {
        self.state.write().await.trades.insert(symbol, trades);
    }

    pub async fn placed_orders(&self) -> Vec<OrderRequest> {
        self.state.read().await.placed_orders.clone()
    }

    pub async fn push_market_event(&self, event: BinanceMarketEvent) {
        self.state.write().await.market_events.push_back(event);
    }

    pub async fn push_account_event(&self, event: BinanceUserStreamEvent) {
        self.state.write().await.account_events.push_back(event);
    }

    pub async fn push_market_status(&self, status: StreamStatus) {
        self.state.write().await.market_status_events.push_back(status);
    }

    pub async fn push_user_status(&self, status: StreamStatus) {
        self.state.write().await.user_status_events.push_back(status);
    }

    pub async fn emit_market_stream_event(&self, event: BinanceMarketEvent) {
        let senders = self.state.read().await.market_streams.senders.clone();
        broadcast_event(senders, event).await;
    }

    pub async fn emit_user_stream_event(&self, event: BinanceUserStreamEvent) {
        let senders = self.state.read().await.user_streams.senders.clone();
        broadcast_event(senders, event).await;
    }

    pub async fn emit_market_stream_status(&self, status: StreamStatus) {
        let senders = self.state.read().await.market_status_streams.senders.clone();
        broadcast_event(senders, status).await;
    }

    pub async fn emit_user_stream_status(&self, status: StreamStatus) {
        let senders = self.state.read().await.user_status_streams.senders.clone();
        broadcast_event(senders, status).await;
    }
}

#[async_trait]
impl BinanceSpotGateway for MockBinanceSpotGateway {
    async fn sync_clock(&self) -> Result<Timestamp> {
        Ok(self.state.read().await.clock_time)
    }

    async fn ping_rest(&self) -> Result<()> {
        Ok(())
    }

    async fn fetch_account_snapshot(&self) -> Result<AccountSnapshot> {
        self.state
            .read()
            .await
            .account
            .clone()
            .ok_or_else(|| anyhow!("mock account snapshot not seeded"))
    }

    async fn fetch_open_orders(&self, symbol: Symbol) -> Result<Vec<OpenOrder>> {
        Ok(self
            .state
            .read()
            .await
            .open_orders
            .get(&symbol)
            .cloned()
            .unwrap_or_default())
    }

    async fn fetch_recent_fills(&self, symbol: Symbol, limit: usize) -> Result<Vec<FillEvent>> {
        Ok(self
            .state
            .read()
            .await
            .fills
            .get(&symbol)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .rev()
            .take(limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect())
    }

    async fn fetch_orderbook_snapshot(&self, symbol: Symbol, _depth: usize) -> Result<OrderBookSnapshot> {
        self.state
            .read()
            .await
            .orderbooks
            .get(&symbol)
            .cloned()
            .ok_or_else(|| anyhow!("mock order book not seeded for {}", symbol))
    }

    async fn fetch_recent_trades(&self, symbol: Symbol, limit: usize) -> Result<Vec<MarketTrade>> {
        Ok(self
            .state
            .read()
            .await
            .trades
            .get(&symbol)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .rev()
            .take(limit)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect())
    }

    async fn fetch_bootstrap_state(&self, symbols: &[Symbol]) -> Result<BinanceBootstrapState> {
        let account = self.fetch_account_snapshot().await?;
        let mut open_orders = Vec::new();
        let mut fills = Vec::new();
        for &symbol in symbols {
            open_orders.extend(self.fetch_open_orders(symbol).await?);
            fills.extend(self.fetch_recent_fills(symbol, 20).await?);
        }

        Ok(BinanceBootstrapState {
            account,
            open_orders,
            fills,
            fetched_at: self.state.read().await.clock_time,
        })
    }

    async fn poll_market_events(&self, symbols: &[Symbol]) -> Result<Vec<BinanceMarketEvent>> {
        let mut state = self.state.write().await;
        if !state.market_events.is_empty() {
            return Ok(state.market_events.drain(..).collect());
        }

        let mut events = Vec::new();
        for &symbol in symbols {
            if let Some(book) = state.orderbooks.get(&symbol).cloned() {
                events.push(BinanceMarketEvent::OrderBookSnapshot(book));
            }
            if let Some(trade) = state.trades.get(&symbol).and_then(|trades| trades.last()).cloned() {
                events.push(BinanceMarketEvent::Trade(trade));
            }
        }
        Ok(events)
    }

    async fn poll_account_events(&self, _symbols: &[Symbol]) -> Result<Vec<BinanceUserStreamEvent>> {
        let mut state = self.state.write().await;
        if !state.account_events.is_empty() {
            return Ok(state.account_events.drain(..).collect());
        }

        let mut events = Vec::new();
        if let Some(account) = state.account.clone() {
            events.push(BinanceUserStreamEvent::AccountSnapshot(account));
        }
        Ok(events)
    }

    async fn place_order(&self, request: OrderRequest) -> Result<domain::ExecutionReport> {
        let mut state = self.state.write().await;
        state.placed_orders.push(request.clone());

        let report = domain::ExecutionReport {
            client_order_id: request.client_order_id.clone(),
            exchange_order_id: Some(format!("mock-{}", state.placed_orders.len())),
            symbol: request.symbol,
            status: if matches!(request.order_type, OrderType::Market) {
                OrderStatus::Filled
            } else {
                OrderStatus::New
            },
            filled_quantity: if matches!(request.order_type, OrderType::Market) {
                request.quantity
            } else {
                Decimal::ZERO
            },
            average_fill_price: request.price,
            fill_ratio: if matches!(request.order_type, OrderType::Market) {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
            requested_price: request.price,
            slippage_bps: None,
            decision_latency_ms: Some(0),
            message: Some("mock order accepted".to_string()),
            event_time: state.clock_time,
        };

        if let Some(open_order) = report.to_open_order(&request) {
            state
                .open_orders
                .entry(request.symbol)
                .or_default()
                .retain(|order| order.client_order_id != open_order.client_order_id);
            state
                .open_orders
                .entry(request.symbol)
                .or_default()
                .push(open_order);
        }

        Ok(report)
    }

    async fn cancel_all_orders(&self, symbol: Symbol) -> Result<()> {
        self.state.write().await.open_orders.remove(&symbol);
        Ok(())
    }

    async fn open_market_stream(&self, _symbols: &[Symbol]) -> Result<MarketStreamHandle> {
        let (event_tx, event_rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let (status_tx, status_rx) = mpsc::channel(128);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        let queued_statuses;
        {
            let mut state = self.state.write().await;
            state.market_streams.senders.push(event_tx.clone());
            state.market_status_streams.senders.push(status_tx.clone());
            queued_statuses = state.market_status_events.drain(..).collect::<Vec<_>>();
        }
        let should_send_default = queued_statuses.is_empty();

        for status in queued_statuses {
            let _ = status_tx.send(status).await;
        }
        if should_send_default {
            let _ = status_tx
                .send(StreamStatus::connected(
                    StreamKind::MarketWs,
                    0,
                    "mock market websocket connected",
                ))
                .await;
        }

        Ok(MarketStreamHandle {
            events: event_rx,
            status: status_rx,
            shutdown: shutdown_tx,
        })
    }

    async fn open_user_stream(&self) -> Result<UserStreamHandle> {
        let (event_tx, event_rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);
        let (status_tx, status_rx) = mpsc::channel(128);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        let queued_statuses;
        {
            let mut state = self.state.write().await;
            state.user_streams.senders.push(event_tx.clone());
            state.user_status_streams.senders.push(status_tx.clone());
            queued_statuses = state.user_status_events.drain(..).collect::<Vec<_>>();
        }
        let should_send_default = queued_statuses.is_empty();

        for status in queued_statuses {
            let _ = status_tx.send(status).await;
        }
        if should_send_default {
            let _ = status_tx
                .send(StreamStatus::connected(
                    StreamKind::UserWs,
                    0,
                    "mock user websocket connected",
                ))
                .await;
        }

        Ok(UserStreamHandle {
            events: event_rx,
            status: status_rx,
            shutdown: shutdown_tx,
        })
    }
}

async fn broadcast_event<T>(senders: Vec<mpsc::Sender<T>>, event: T)
where
    T: Clone + Send + 'static,
{
    for sender in senders {
        let _ = sender.send(event.clone()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_depth_stream_message() {
        let raw = r#"{
            "stream":"btcusdc@depth@100ms",
            "data":{
                "e":"depthUpdate",
                "E":1710000000000,
                "s":"BTCUSDC",
                "U":100,
                "u":102,
                "b":[["60000.10","0.500"],["59999.90","0"]],
                "a":[["60001.20","0.300"]]
            }
        }"#;

        let event = parse_market_stream_message(raw).unwrap().unwrap();
        match event {
            BinanceMarketEvent::OrderBookDelta(delta) => {
                assert_eq!(delta.symbol, Symbol::BtcUsdc);
                assert_eq!(delta.first_update_id, 100);
                assert_eq!(delta.final_update_id, 102);
                assert_eq!(delta.bids.len(), 2);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_execution_report_user_event() {
        let raw = r#"{
            "subscriptionId":0,
            "event":{
                "e":"executionReport",
                "E":1710000000100,
                "s":"BTCUSDC",
                "c":"bot-1",
                "S":"BUY",
                "o":"LIMIT",
                "f":"GTC",
                "q":"0.010",
                "p":"60000.00",
                "x":"TRADE",
                "X":"PARTIALLY_FILLED",
                "r":"NONE",
                "i":42,
                "l":"0.003",
                "z":"0.003",
                "L":"60000.00",
                "n":"0.01",
                "N":"USDC",
                "T":1710000000100,
                "t":7,
                "w":true,
                "m":false,
                "O":1710000000000,
                "Z":"180.00"
            }
        }"#;

        let event = parse_user_stream_message(raw).unwrap().unwrap();
        match event {
            BinanceUserStreamEvent::Execution(execution) => {
                assert_eq!(execution.report.client_order_id, "bot-1");
                assert_eq!(execution.report.status, OrderStatus::PartiallyFilled);
                assert!(execution.fill.is_some());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
