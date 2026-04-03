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
use reqwest::StatusCode;
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
const USER_STREAM_HEARTBEAT_INTERVAL_SECS: u64 = 2;
const FEE_QUOTE_CACHE_TTL_SECS: i64 = 5;

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
    async fn prepare_order(&self, request: OrderRequest) -> Result<crate::PreparedOrder>;
    async fn place_order(&self, request: OrderRequest) -> Result<domain::ExecutionReport>;
    async fn cancel_order(&self, order: &OpenOrder) -> Result<()>;
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
    local_min_notional_quote: Decimal,
    credentials: BinanceCredentials,
    http_client: HttpClient,
    fee_quote_conversion_cache: Arc<RwLock<HashMap<String, FeeQuoteConversionCacheEntry>>>,
    symbol_rules_cache: Arc<RwLock<HashMap<Symbol, SymbolTradingRules>>>,
}

#[derive(Debug, Clone, Copy)]
struct FeeQuoteConversionCacheEntry {
    price: Decimal,
    observed_at: Timestamp,
}

#[derive(Debug, Clone, Copy)]
struct SymbolTradingRules {
    price_filter: Option<PriceFilterRule>,
    lot_size: Option<LotSizeRule>,
    market_lot_size: Option<LotSizeRule>,
    min_notional: Option<MinNotionalRule>,
}

#[derive(Debug, Clone, Copy)]
struct PriceFilterRule {
    min_price: Decimal,
    max_price: Decimal,
    tick_size: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct LotSizeRule {
    min_qty: Decimal,
    max_qty: Decimal,
    step_size: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct MinNotionalRule {
    min_notional: Decimal,
    apply_to_market: bool,
}

impl BinanceSpotClient {
    pub fn new(
        rest_base_url: impl AsRef<str>,
        market_ws_url: impl AsRef<str>,
        user_ws_url: impl AsRef<str>,
        recv_window_ms: u64,
        local_min_notional_quote: Decimal,
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
            local_min_notional_quote,
            credentials,
            http_client: HttpClient::builder().default_headers(headers).build()?,
            fee_quote_conversion_cache: Arc::new(RwLock::new(HashMap::new())),
            symbol_rules_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url> {
        self.rest_base_url
            .join(path)
            .with_context(|| format!("failed to build Binance endpoint for path {path}"))
    }

    fn signed_endpoint(&self, path: &str, extra_query: &[(&str, String)]) -> Result<Url> {
        let query = signed_query(self.recv_window_ms, extra_query);
        let encoded = encode_query(&query);
        let signature = self.sign_rest_query(&encoded)?;
        let mut url = self.endpoint(path)?;
        url.set_query(Some(&format!("{encoded}&signature={signature}")));
        Ok(url)
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

    async fn symbol_trading_rules(&self, symbol: Symbol) -> Result<SymbolTradingRules> {
        if let Some(cached) = self.symbol_rules_cache.read().await.get(&symbol).copied() {
            return Ok(cached);
        }

        let response: BinanceExchangeInfoResponse = self
            .get_public("/api/v3/exchangeInfo", &[("symbol", symbol.as_str().to_string())])
            .await
            .with_context(|| format!("failed to fetch exchangeInfo for {symbol}"))?;

        let symbol_info = response
            .symbols
            .into_iter()
            .find(|entry| entry.symbol.eq_ignore_ascii_case(symbol.as_str()))
            .ok_or_else(|| anyhow!("exchangeInfo returned no symbol entry for {symbol}"))?;
        let rules = parse_symbol_trading_rules(symbol, &symbol_info.filters)?;
        self.symbol_rules_cache.write().await.insert(symbol, rules);
        Ok(rules)
    }

    async fn order_reference_price(&self, request: &OrderRequest) -> Result<Decimal> {
        if let Some(price) = request.price {
            return Ok(price);
        }

        let response: BinanceTickerPriceResponse = self
            .get_public(
                "/api/v3/ticker/price",
                &[("symbol", request.symbol.as_str().to_string())],
            )
            .await
            .with_context(|| format!("failed to fetch ticker price for {}", request.symbol))?;
        parse_decimal(&response.price)
    }

    async fn prepare_order_request(&self, request: OrderRequest) -> Result<crate::PreparedOrder> {
        let rules = self.symbol_trading_rules(request.symbol).await?;
        let mut normalized = request.clone();

        if normalized.quantity <= Decimal::ZERO {
            return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                &request,
                format!(
                    "local order rejection: quantity must be > 0 (quantity={})",
                    request.quantity
                ),
            )));
        }

        if matches!(normalized.order_type, OrderType::Limit | OrderType::LimitMaker) {
            let Some(price) = normalized.price else {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!(
                        "local order rejection: {} requires a positive limit price",
                        serialize_order_type(normalized.order_type)
                    ),
                )));
            };

            if price <= Decimal::ZERO {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!("local order rejection: price must be > 0 (price={price})"),
                )));
            }
        }

        if let (Some(price), Some(price_filter)) = (normalized.price, rules.price_filter) {
            let normalized_price = normalize_price_to_tick(price, price_filter.tick_size, normalized.side);
            if normalized_price <= Decimal::ZERO {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!(
                        "local order rejection: price collapsed after tick normalization (original_price={price}, tick_size={})",
                        price_filter.tick_size
                    ),
                )));
            }
            if normalized_price < price_filter.min_price && price_filter.min_price > Decimal::ZERO {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!(
                        "local order rejection: normalized price fell below PRICE_FILTER minPrice (normalized_price={normalized_price}, min_price={})",
                        price_filter.min_price
                    ),
                )));
            }
            if normalized_price > price_filter.max_price && price_filter.max_price > Decimal::ZERO {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!(
                        "local order rejection: normalized price exceeded PRICE_FILTER maxPrice (normalized_price={normalized_price}, max_price={})",
                        price_filter.max_price
                    ),
                )));
            }
            normalized.price = Some(normalized_price);
        }

        let lot_size = if matches!(normalized.order_type, OrderType::Market) {
            rules.market_lot_size.or(rules.lot_size)
        } else {
            rules.lot_size
        };

        if let Some(lot_size) = lot_size {
            let normalized_quantity = normalize_quantity_to_step(normalized.quantity, lot_size.step_size);
            if normalized_quantity <= Decimal::ZERO {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!(
                        "local order rejection: quantity collapsed after LOT_SIZE normalization (original_qty={}, step_size={})",
                        request.quantity,
                        lot_size.step_size
                    ),
                )));
            }
            if normalized_quantity < lot_size.min_qty {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!(
                        "local order rejection: normalized quantity below LOT_SIZE minQty (normalized_qty={normalized_quantity}, min_qty={}, step_size={})",
                        lot_size.min_qty,
                        lot_size.step_size
                    ),
                )));
            }
            if lot_size.max_qty > Decimal::ZERO && normalized_quantity > lot_size.max_qty {
                return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                    &request,
                    format!(
                        "local order rejection: normalized quantity above LOT_SIZE maxQty (normalized_qty={normalized_quantity}, max_qty={})",
                        lot_size.max_qty
                    ),
                )));
            }
            normalized.quantity = normalized_quantity;
        }

        let reference_price = self.order_reference_price(&normalized).await?;
        if reference_price <= Decimal::ZERO {
            return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                &request,
                format!(
                    "local order rejection: reference price must be > 0 (reference_price={reference_price})"
                ),
            )));
        }

        let exchange_min_notional = rules.min_notional.and_then(|min_notional| {
            (!matches!(normalized.order_type, OrderType::Market) || min_notional.apply_to_market)
                .then_some(min_notional.min_notional)
        });
        let effective_local_min_notional = exchange_min_notional
            .map(|value| value.max(self.local_min_notional_quote))
            .unwrap_or(self.local_min_notional_quote);
        let notional = reference_price * normalized.quantity;
        if notional < effective_local_min_notional {
            let exchange_floor = exchange_min_notional.unwrap_or(Decimal::ZERO);
            return Ok(crate::PreparedOrder::Rejected(rejected_execution_report(
                &request,
                format!(
                    "local order rejection: notional below effective local minNotional floor (notional={notional}, effective_min_notional={}, local_min_notional={}, exchange_min_notional={}, reference_price={reference_price}, normalized_qty={})",
                    effective_local_min_notional,
                    self.local_min_notional_quote,
                    exchange_floor,
                    normalized.quantity
                ),
            )));
        }

        if normalized.price != request.price || normalized.quantity != request.quantity {
            warn!(
                symbol = %normalized.symbol,
                side = ?normalized.side,
                original_price = ?request.price,
                normalized_price = ?normalized.price,
                original_quantity = %request.quantity,
                normalized_quantity = %normalized.quantity,
                "normalized order request to Binance spot filters before submit"
            );
        }

        Ok(crate::PreparedOrder::Ready(normalized))
    }

    async fn quote_conversion_price(&self, base_asset: &str, quote_asset: &str) -> Result<Decimal> {
        if base_asset.eq_ignore_ascii_case(quote_asset) {
            return Ok(Decimal::ONE);
        }

        let symbol = format!(
            "{}{}",
            base_asset.to_ascii_uppercase(),
            quote_asset.to_ascii_uppercase()
        );
        if let Some(cached_price) = self.cached_quote_conversion_price(&symbol).await {
            return Ok(cached_price);
        }

        let response: BinanceTickerPriceResponse = self
            .get_public("/api/v3/ticker/price", &[("symbol", symbol.clone())])
            .await?;
        let price = parse_decimal(&response.price)?;

        self.fee_quote_conversion_cache.write().await.insert(
            symbol,
            FeeQuoteConversionCacheEntry {
                price,
                observed_at: now_utc(),
            },
        );

        Ok(price)
    }

    async fn cached_quote_conversion_price(&self, symbol: &str) -> Option<Decimal> {
        let cache = self.fee_quote_conversion_cache.read().await;
        let entry = cache.get(symbol)?;
        let age = now_utc() - entry.observed_at;
        (age <= time::Duration::seconds(FEE_QUOTE_CACHE_TTL_SECS)).then_some(entry.price)
    }

    async fn enrich_fill_fee_quote(&self, mut fill: FillEvent) -> Result<FillEvent> {
        if fill.fee_quote.is_some() {
            return Ok(fill);
        }

        if let Some(fee_quote) = inferred_fee_quote(fill.symbol, &fill.fee_asset, fill.fee, fill.price) {
            fill.fee_quote = Some(fee_quote);
            return Ok(fill);
        }

        let conversion_price = self
            .quote_conversion_price(&fill.fee_asset, quote_asset_for_symbol(fill.symbol))
            .await?;
        fill.fee_quote = Some(fill.fee * conversion_price);
        Ok(fill)
    }

    async fn enrich_fills_fee_quote(&self, fills: Vec<FillEvent>) -> Result<Vec<FillEvent>> {
        let mut enriched = Vec::with_capacity(fills.len());
        for fill in fills {
            enriched.push(self.enrich_fill_fee_quote(fill).await?);
        }
        Ok(enriched)
    }

    async fn enrich_user_stream_event(&self, event: BinanceUserStreamEvent) -> Result<BinanceUserStreamEvent> {
        match event {
            BinanceUserStreamEvent::Execution(mut execution) => {
                if let Some(fill) = execution.fill.take() {
                    execution.fill = Some(self.enrich_fill_fee_quote(fill).await?);
                }
                Ok(BinanceUserStreamEvent::Execution(execution))
            }
            other => Ok(other),
        }
    }

    fn mark_fill_recovery_after_enrichment_failure(
        &self,
        event: BinanceUserStreamEvent,
        error: &anyhow::Error,
    ) -> BinanceUserStreamEvent {
        match event {
            BinanceUserStreamEvent::Execution(mut execution) => {
                if let Some(fill) = execution.fill.take() {
                    let detail = format!("fill queued for REST recovery after fee quote enrichment failure: {error}");
                    execution.report.message = Some(match execution.report.message.take() {
                        Some(existing) if !existing.is_empty() => format!("{existing}; {detail}"),
                        _ => detail.clone(),
                    });
                    execution.fill_recovery = Some(crate::BinanceFillRecoveryRequest {
                        symbol: fill.symbol,
                        trade_id: fill.trade_id,
                        order_id: fill.order_id,
                        fee_asset: fill.fee_asset,
                        event_time: fill.event_time,
                        reason: detail,
                    });
                }
                BinanceUserStreamEvent::Execution(execution)
            }
            other => other,
        }
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
        let url = self.signed_endpoint(path, extra_query)?;
        let response = self.http_client.get(url).send().await?.error_for_status()?;
        Ok(response.json::<T>().await?)
    }

    async fn signed_post_response(
        &self,
        path: &str,
        extra_query: &[(&str, String)],
    ) -> Result<reqwest::Response> {
        let query = signed_query(self.recv_window_ms, extra_query);
        let encoded = encode_query(&query);
        let signature = self.sign_rest_query(&encoded)?;
        let payload = format!("{encoded}&signature={signature}");

        Ok(self
            .http_client
            .post(self.endpoint(path)?)
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(payload)
            .send()
            .await?)
    }

    async fn signed_delete_empty(&self, path: &str, extra_query: &[(&str, String)]) -> Result<()> {
        let url = self.signed_endpoint(path, extra_query)?;
        let response = self.http_client.delete(url).send().await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let body = response.text().await.unwrap_or_default();
        bail!("Binance signed DELETE failed: {}", format_binance_rest_error(status, &body));
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

        let fills = response
            .into_iter()
            .map(|trade| parse_my_trade_response(symbol, trade))
            .collect::<Result<Vec<_>>>()?;
        self.enrich_fills_fee_quote(fills).await
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

    async fn prepare_order(&self, request: OrderRequest) -> Result<crate::PreparedOrder> {
        self.prepare_order_request(request).await
    }

    async fn place_order(&self, request: OrderRequest) -> Result<domain::ExecutionReport> {
        let params = build_order_params(&request);
        let response = self.signed_post_response("/api/v3/order", &params).await?;
        let status = response.status();

        if status.is_success() {
            let response: BinanceOrderResponse = parse_json_response(response).await?;
            return parse_order_response(request.symbol, response);
        }

        let body = response.text().await.unwrap_or_default();
        let detail = format_binance_rest_error(status, &body);

        warn!(
            symbol = %request.symbol,
            side = ?request.side,
            order_type = %serialize_order_type(request.order_type),
            price = ?request.price,
            quantity = %request.quantity,
            status = status.as_u16(),
            body = %body,
            "Binance rejected order REST request"
        );

        if status == StatusCode::BAD_REQUEST {
            return Ok(rejected_execution_report(&request, detail));
        }

        bail!("Binance order placement failed: {detail}");
    }

    async fn cancel_all_orders(&self, symbol: Symbol) -> Result<()> {
        self.signed_delete_empty(
            "/api/v3/openOrders",
            &[("symbol", symbol.as_str().to_string())],
        )
        .await
        .with_context(|| format!("failed to cancel all open orders for {symbol}"))
    }

    async fn cancel_order(&self, order: &OpenOrder) -> Result<()> {
        self.signed_delete_empty(
            "/api/v3/order",
            &[
                ("symbol", order.symbol.as_str().to_string()),
                ("origClientOrderId", order.client_order_id.clone()),
            ],
        )
        .await
        .with_context(|| {
            format!(
                "failed to cancel order {} for {}",
                order.client_order_id, order.symbol
            )
        })
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
struct BinanceTickerPriceResponse {
    price: String,
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

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoResponse {
    symbols: Vec<BinanceExchangeInfoSymbol>,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoSymbol {
    symbol: String,
    filters: Vec<Value>,
}

#[derive(Debug, Deserialize)]
struct BinanceRestErrorResponse {
    code: i64,
    msg: String,
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
                let subscription_request = match client.websocket_user_stream_request() {
                    Ok(request) => request,
                    Err(error) => {
                        warn!(?error, "failed to build user websocket subscription request");
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
                        continue;
                    }
                };

                if let Err(error) = writer.send(Message::Text(subscription_request.into())).await {
                    warn!(?error, "failed to subscribe user websocket");
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
                    continue;
                }

                let mut subscription_confirmed = false;
                let mut ping = interval(Duration::from_secs(USER_STREAM_HEARTBEAT_INTERVAL_SECS));
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
                                Some(Ok(Message::Text(text))) => {
                                    match parse_user_stream_response(&text) {
                                        Ok(Some(response)) => {
                                            if response.success {
                                                subscription_confirmed = true;
                                                let _ = status_tx
                                                    .send(StreamStatus::connected(
                                                        StreamKind::UserWs,
                                                        reconnect_count,
                                                        response.detail,
                                                    ))
                                                    .await;
                                            } else {
                                                warn!(
                                                    status = response.status,
                                                    detail = %response.detail,
                                                    body = %text,
                                                    "user websocket subscription rejected by exchange"
                                                );
                                                break;
                                            }
                                            continue;
                                        }
                                        Ok(None) => {}
                                        Err(error) => {
                                            warn!(?error, body = %text, "failed to parse user websocket response");
                                            continue;
                                        }
                                    }

                                    match parse_user_stream_message(&text) {
                                        Ok(Some(event)) => {
                                            if !subscription_confirmed {
                                                subscription_confirmed = true;
                                                let _ = status_tx
                                                    .send(StreamStatus::connected(
                                                        StreamKind::UserWs,
                                                        reconnect_count,
                                                        "user websocket streaming events confirmed",
                                                    ))
                                                    .await;
                                            }
                                            let event = match client.enrich_user_stream_event(event.clone()).await {
                                                Ok(event) => event,
                                                Err(error) => {
                                                    warn!(?error, "failed to enrich user event fill fee quote; queueing fill for REST recovery");
                                                    client.mark_fill_recovery_after_enrichment_failure(event, &error)
                                                }
                                            };
                                            if event_tx.send(event).await.is_err() {
                                                return;
                                            }
                                        }
                                        Ok(None) => {}
                                        Err(error) => warn!(?error, body = %text, "failed to parse user websocket message"),
                                    }
                                }
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(error) = writer.send(Message::Pong(payload)).await {
                                        warn!(?error, "failed to answer user ping");
                                        break;
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    if subscription_confirmed {
                                        let _ = status_tx
                                            .send(StreamStatus::connected(
                                                StreamKind::UserWs,
                                                reconnect_count,
                                                "user websocket heartbeat pong",
                                            ))
                                            .await;
                                    }
                                }
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
    if payload.get("status").is_some() || payload.get("result").is_some() || payload.get("id").is_some() {
        return Ok(None);
    }

    let Some(event) = payload
        .get("event")
        .or_else(|| payload.get("data"))
        .or_else(|| payload.get("e").map(|_| &payload))
    else {
        return Ok(None);
    };

    if event.get("e").is_none() {
        return Ok(None);
    }

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserStreamResponse {
    status: u64,
    success: bool,
    detail: String,
}

fn parse_user_stream_response(raw: &str) -> Result<Option<UserStreamResponse>> {
    let payload: Value = serde_json::from_str(raw)?;
    let Some(status) = payload.get("status").and_then(Value::as_u64) else {
        return Ok(None);
    };

    let subscription_id = payload
        .get("result")
        .and_then(|result| result.get("subscriptionId"))
        .and_then(Value::as_u64);
    let success = (200..300).contains(&status);
    let detail = if success {
        match subscription_id {
            Some(subscription_id) => {
                format!("user websocket subscription confirmed (subscription_id={subscription_id})")
            }
            None => "user websocket request acknowledged".to_string(),
        }
    } else {
        let error = payload.get("error");
        let error_code = error
            .and_then(|error| error.get("code"))
            .and_then(Value::as_i64);
        let error_message = error
            .and_then(|error| error.get("msg").and_then(Value::as_str))
            .or_else(|| error.and_then(|error| error.get("message").and_then(Value::as_str)))
            .unwrap_or("unknown websocket API error");

        match error_code {
            Some(error_code) => {
                format!("user websocket request rejected: code={error_code} msg={error_message}")
            }
            None => format!("user websocket request rejected: status={status} msg={error_message}"),
        }
    };

    Ok(Some(UserStreamResponse {
        status,
        success,
        detail,
    }))
}

async fn parse_json_response<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        bail!("Binance REST request failed: {}", format_binance_rest_error(status, &body));
    }

    serde_json::from_str(&body).with_context(|| {
        format!(
            "failed to decode Binance REST success payload (status={} body={body})",
            status.as_u16()
        )
    })
}

fn build_order_params(request: &OrderRequest) -> Vec<(&'static str, String)> {
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
        if !matches!(request.order_type, OrderType::LimitMaker) {
            params.push(("timeInForce", serialize_time_in_force(time_in_force).to_string()));
        }
    }

    params
}

fn parse_symbol_trading_rules(symbol: Symbol, filters: &[Value]) -> Result<SymbolTradingRules> {
    let mut rules = SymbolTradingRules {
        price_filter: None,
        lot_size: None,
        market_lot_size: None,
        min_notional: None,
    };

    for filter in filters {
        let Some(filter_type) = filter.get("filterType").and_then(Value::as_str) else {
            continue;
        };

        match filter_type {
            "PRICE_FILTER" => {
                rules.price_filter = Some(PriceFilterRule {
                    min_price: parse_decimal_value(filter, "minPrice").unwrap_or(Decimal::ZERO),
                    max_price: parse_decimal_value(filter, "maxPrice").unwrap_or(Decimal::ZERO),
                    tick_size: parse_decimal_value(filter, "tickSize").unwrap_or(Decimal::ZERO),
                });
            }
            "LOT_SIZE" => {
                rules.lot_size = Some(LotSizeRule {
                    min_qty: parse_decimal_value(filter, "minQty").unwrap_or(Decimal::ZERO),
                    max_qty: parse_decimal_value(filter, "maxQty").unwrap_or(Decimal::ZERO),
                    step_size: parse_decimal_value(filter, "stepSize").unwrap_or(Decimal::ZERO),
                });
            }
            "MARKET_LOT_SIZE" => {
                rules.market_lot_size = Some(LotSizeRule {
                    min_qty: parse_decimal_value(filter, "minQty").unwrap_or(Decimal::ZERO),
                    max_qty: parse_decimal_value(filter, "maxQty").unwrap_or(Decimal::ZERO),
                    step_size: parse_decimal_value(filter, "stepSize").unwrap_or(Decimal::ZERO),
                });
            }
            "MIN_NOTIONAL" => {
                rules.min_notional = Some(MinNotionalRule {
                    min_notional: parse_decimal_value(filter, "minNotional").unwrap_or(Decimal::ZERO),
                    apply_to_market: filter
                        .get("applyToMarket")
                        .and_then(Value::as_bool)
                        .unwrap_or(true),
                });
            }
            "NOTIONAL" => {
                rules.min_notional = Some(MinNotionalRule {
                    min_notional: parse_decimal_value(filter, "minNotional").unwrap_or(Decimal::ZERO),
                    apply_to_market: filter
                        .get("applyMinToMarket")
                        .and_then(Value::as_bool)
                        .unwrap_or(true),
                });
            }
            _ => {}
        }
    }

    if rules.lot_size.is_none() {
        bail!("exchangeInfo for {symbol} did not include LOT_SIZE");
    }

    Ok(rules)
}

fn parse_decimal_value(value: &Value, field: &str) -> Option<Decimal> {
    value
        .get(field)
        .and_then(Value::as_str)
        .and_then(|raw| Decimal::from_str_exact(raw).ok())
}

fn normalize_price_to_tick(price: Decimal, tick_size: Decimal, side: Side) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return price;
    }

    let scaled = price / tick_size;
    let units = match side {
        Side::Buy => scaled.floor(),
        Side::Sell => scaled.ceil(),
    };
    (units * tick_size).normalize()
}

fn normalize_quantity_to_step(quantity: Decimal, step_size: Decimal) -> Decimal {
    if step_size <= Decimal::ZERO {
        return quantity;
    }

    ((quantity / step_size).floor() * step_size).normalize()
}

fn rejected_execution_report(request: &OrderRequest, reason: impl Into<String>) -> domain::ExecutionReport {
    domain::ExecutionReport {
        client_order_id: request.client_order_id.clone(),
        exchange_order_id: None,
        symbol: request.symbol,
        status: OrderStatus::Rejected,
        filled_quantity: Decimal::ZERO,
        average_fill_price: None,
        fill_ratio: Decimal::ZERO,
        requested_price: request.price,
        slippage_bps: None,
        decision_latency_ms: None,
        submit_ack_latency_ms: None,
        submit_to_first_report_ms: None,
        submit_to_fill_ms: None,
        exchange_order_age_ms: None,
        intent_role: Some(request.intent_role),
        exit_stage: request.exit_stage,
        exit_reason: request.exit_reason.clone(),
        message: Some(reason.into()),
        event_time: now_utc(),
    }
}

fn format_binance_rest_error(status: StatusCode, body: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<BinanceRestErrorResponse>(body) {
        return format!(
            "status={} code={} msg={}",
            status.as_u16(),
            parsed.code,
            parsed.msg
        );
    }

    format!("status={} body={body}", status.as_u16())
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
        intent_role: domain::IntentRole::AddRisk,
        exit_stage: None,
        exit_reason: None,
        updated_at: millis_timestamp(response.time as i64)?,
    })
}

fn parse_my_trade_response(symbol: Symbol, trade: BinanceMyTradeResponse) -> Result<FillEvent> {
    let price = parse_decimal(&trade.price)?;
    let fee = parse_decimal(&trade.commission)?;
    let fee_asset = trade.commission_asset;
    Ok(FillEvent {
        trade_id: trade.id.to_string(),
        order_id: trade.order_id.to_string(),
        symbol,
        side: if trade.is_buyer { Side::Buy } else { Side::Sell },
        price,
        quantity: parse_decimal(&trade.qty)?,
        fee,
        fee_quote: inferred_fee_quote(symbol, &fee_asset, fee, price),
        fee_asset,
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
        submit_ack_latency_ms: None,
        submit_to_first_report_ms: None,
        submit_to_fill_ms: None,
        exchange_order_age_ms: None,
        intent_role: None,
        exit_stage: None,
        exit_reason: None,
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
        let fee = parse_decimal(required_str(event, "n")?)?;
        let fee_asset = event
            .get("N")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_string();
        let price = last_executed_price.unwrap_or(Decimal::ZERO);
        Some(FillEvent {
            trade_id: trade_id.to_string(),
            order_id: required_u64(event, "i")?.to_string(),
            symbol,
            side: parse_side(required_str(event, "S")?)?,
            price,
            quantity: last_executed_quantity,
            fee,
            fee_quote: inferred_fee_quote(symbol, &fee_asset, fee, price),
            fee_asset,
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
            decision_latency_ms: None,
            submit_ack_latency_ms: None,
            submit_to_first_report_ms: None,
            submit_to_fill_ms: None,
            exchange_order_age_ms: event
                .get("O")
                .and_then(Value::as_i64)
                .map(|created| required_i64(event, "E").map(|event_time| event_time - created))
                .transpose()?,
            intent_role: None,
            exit_stage: None,
            exit_reason: None,
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
        fill_recovery: None,
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

fn inferred_fee_quote(symbol: Symbol, fee_asset: &str, fee: Decimal, fill_price: Decimal) -> Option<Decimal> {
    if fee.is_zero() {
        Some(Decimal::ZERO)
    } else if fee_asset.eq_ignore_ascii_case(quote_asset_for_symbol(symbol)) {
        Some(fee)
    } else if fee_asset.eq_ignore_ascii_case(base_asset_for_symbol(symbol)) {
        Some(fee * fill_price)
    } else {
        None
    }
}

fn base_asset_for_symbol(symbol: Symbol) -> &'static str {
    match symbol {
        Symbol::BtcUsdc => "BTC",
        Symbol::EthUsdc => "ETH",
    }
}

fn quote_asset_for_symbol(symbol: Symbol) -> &'static str {
    match symbol {
        Symbol::BtcUsdc | Symbol::EthUsdc => "USDC",
    }
}

fn parse_decimal(raw: &str) -> Result<Decimal> {
    Decimal::from_str_exact(raw).with_context(|| format!("invalid decimal `{raw}`"))
}

fn optional_decimal(raw: &str) -> Result<Option<Decimal>> {
    let decimal = parse_decimal(raw)?;
    Ok((!decimal.is_zero()).then_some(decimal))
}

fn average_fill_price(filled_quantity: Decimal, quote_quantity: Decimal) -> Option<Decimal> {
    if filled_quantity.is_zero() {
        None
    } else {
        Some(quote_quantity / filled_quantity)
    }
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

    async fn prepare_order(&self, request: OrderRequest) -> Result<crate::PreparedOrder> {
        Ok(crate::PreparedOrder::Ready(request))
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
            submit_ack_latency_ms: Some(0),
            submit_to_first_report_ms: Some(0),
            submit_to_fill_ms: if matches!(request.order_type, OrderType::Market) {
                Some(0)
            } else {
                None
            },
            exchange_order_age_ms: None,
            intent_role: Some(request.intent_role),
            exit_stage: request.exit_stage,
            exit_reason: request.exit_reason.clone(),
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

    async fn cancel_order(&self, order: &OpenOrder) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(orders) = state.open_orders.get_mut(&order.symbol) {
            orders.retain(|candidate| candidate.client_order_id != order.client_order_id);
        }
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
    
    fn dec(raw: &str) -> Decimal {
        Decimal::from_str_exact(raw).unwrap()
    }

    fn sample_client() -> BinanceSpotClient {
        BinanceSpotClient::new(
            "http://127.0.0.1:1",
            "ws://127.0.0.1:1/market",
            "ws://127.0.0.1:1/user",
            5_000,
            dec("10.00"),
            BinanceCredentials {
                api_key: "test-key".to_string(),
                api_secret: "test-secret".to_string(),
            },
        )
        .unwrap()
    }

    fn execution_report_event_raw(fee_asset: &str) -> String {
        format!(
            r#"{{
            "subscriptionId":0,
            "event":{{
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
                "N":"{fee_asset}",
                "T":1710000000100,
                "t":7,
                "w":true,
                "m":false,
                "O":1710000000000,
                "Z":"180.00"
            }}
        }}"#
        )
    }

    #[test]
    fn parses_user_stream_subscription_response() {
        let response = parse_user_stream_response(
            r#"{
                "id":"req-1",
                "status":200,
                "result":{"subscriptionId":7}
            }"#,
        )
        .unwrap()
        .expect("subscription response should parse");

        assert!(response.success);
        assert_eq!(response.status, 200);
        assert!(response.detail.contains("subscription_id=7"));
        assert!(parse_user_stream_message(r#"{"id":"req-1","status":200,"result":{"subscriptionId":7}}"#)
            .unwrap()
            .is_none());
    }

    #[test]
    fn parses_user_stream_subscription_rejection() {
        let response = parse_user_stream_response(
            r#"{
                "id":"req-1",
                "status":400,
                "error":{"code":-1102,"msg":"Mandatory parameter 'timestamp' was not sent"}
            }"#,
        )
        .unwrap()
        .expect("subscription error response should parse");

        assert!(!response.success);
        assert_eq!(response.status, 400);
        assert!(response.detail.contains("-1102"));
        assert!(response.detail.contains("timestamp"));
    }

    #[test]
    fn ignores_non_event_user_stream_control_payload() {
        assert!(parse_user_stream_message(r#"{"id":"req-1","result":{"subscriptionId":7}}"#)
            .unwrap()
            .is_none());
    }

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
    fn limit_maker_order_params_omit_time_in_force() {
        let params = build_order_params(&OrderRequest {
            client_order_id: "bot-1".to_string(),
            symbol: Symbol::BtcUsdc,
            side: Side::Buy,
            order_type: OrderType::LimitMaker,
            price: Some(dec("60000.01")),
            quantity: dec("0.00024"),
            time_in_force: Some(TimeInForce::Gtc),
            post_only: true,
            reduce_only: false,
            source_intent_id: "intent-1".to_string(),
        });

        assert!(!params.iter().any(|(key, _)| *key == "timeInForce"));
    }

    #[test]
    fn parses_symbol_filters_and_normalizes_price_and_quantity() {
        let filters = serde_json::from_str::<Vec<Value>>(
            r#"[
                {
                    "filterType":"PRICE_FILTER",
                    "minPrice":"0.01000000",
                    "maxPrice":"1000000.00000000",
                    "tickSize":"0.01000000"
                },
                {
                    "filterType":"LOT_SIZE",
                    "minQty":"0.00001000",
                    "maxQty":"1000.00000000",
                    "stepSize":"0.00001000"
                },
                {
                    "filterType":"MIN_NOTIONAL",
                    "minNotional":"10.00000000",
                    "applyToMarket":true,
                    "avgPriceMins":5
                }
            ]"#,
        )
        .unwrap();

        let rules = parse_symbol_trading_rules(Symbol::BtcUsdc, &filters).unwrap();
        let normalized_price = normalize_price_to_tick(dec("60000.019"), rules.price_filter.unwrap().tick_size, Side::Buy);
        let normalized_quantity = normalize_quantity_to_step(dec("0.0002474724209577209625645295"), rules.lot_size.unwrap().step_size);

        assert_eq!(normalized_price, dec("60000.01"));
        assert_eq!(normalized_quantity, dec("0.00024"));
    }

    #[test]
    fn formats_binance_rest_error_with_code_and_message() {
        let detail = format_binance_rest_error(
            StatusCode::BAD_REQUEST,
            r#"{"code":-1013,"msg":"Filter failure: LOT_SIZE"}"#,
        );

        assert!(detail.contains("400"));
        assert!(detail.contains("-1013"));
        assert!(detail.contains("LOT_SIZE"));
    }

    #[test]
    fn signed_endpoint_puts_symbol_and_signature_in_query_string() {
        let client = sample_client();
        let url = client
            .signed_endpoint("/api/v3/openOrders", &[("symbol", Symbol::BtcUsdc.as_str().to_string())])
            .unwrap();
        let query = url.query().unwrap_or_default();

        assert!(url.as_str().starts_with("http://127.0.0.1:1/api/v3/openOrders?"));
        assert!(query.contains("symbol=BTCUSDC"));
        assert!(query.contains("recvWindow=5000"));
        assert!(query.contains("timestamp="));
        assert!(query.contains("signature="));
    }

    #[test]
    fn average_fill_price_returns_none_for_zero_filled_quantity() {
        assert_eq!(average_fill_price(Decimal::ZERO, dec("12.34")), None);
    }

    #[test]
    fn parse_order_response_accepts_new_open_order_without_fill_quantity() {
        let report = parse_order_response(
            Symbol::BtcUsdc,
            BinanceOrderResponse {
                symbol: "BTCUSDC".to_string(),
                order_id: 42,
                client_order_id: "bot-1".to_string(),
                status: "NEW".to_string(),
                executed_quantity: "0".to_string(),
                cumulative_quote_quantity: "0".to_string(),
                transact_time: 1_710_000_000_000,
            },
        )
        .unwrap();

        assert_eq!(report.status, OrderStatus::New);
        assert_eq!(report.filled_quantity, Decimal::ZERO);
        assert_eq!(report.average_fill_price, None);
    }

    #[tokio::test]
    async fn prepare_order_request_normalizes_limit_maker_request_locally() {
        let client = sample_client();
        client.symbol_rules_cache.write().await.insert(
            Symbol::BtcUsdc,
            SymbolTradingRules {
                price_filter: Some(PriceFilterRule {
                    min_price: dec("0.01"),
                    max_price: dec("1000000"),
                    tick_size: dec("0.01"),
                }),
                lot_size: Some(LotSizeRule {
                    min_qty: dec("0.00001"),
                    max_qty: dec("1000"),
                    step_size: dec("0.00001"),
                }),
                market_lot_size: None,
                min_notional: Some(MinNotionalRule {
                    min_notional: dec("10"),
                    apply_to_market: true,
                }),
            },
        );

        let prepared = client
            .prepare_order_request(OrderRequest {
                client_order_id: "bot-1".to_string(),
                symbol: Symbol::BtcUsdc,
                side: Side::Buy,
                order_type: OrderType::LimitMaker,
                price: Some(dec("60000.019")),
                quantity: dec("0.0002474724209577209625645295"),
                time_in_force: Some(TimeInForce::Gtc),
                post_only: true,
                reduce_only: false,
                source_intent_id: "intent-1".to_string(),
            })
            .await
            .unwrap();

        match prepared {
            crate::PreparedOrder::Ready(request) => {
                assert_eq!(request.price, Some(dec("60000.01")));
                assert_eq!(request.quantity, dec("0.00024"));
                assert_eq!(request.time_in_force, Some(TimeInForce::Gtc));
            }
            other => panic!("unexpected prepared order: {other:?}"),
        }
    }

    #[tokio::test]
    async fn prepare_order_request_rejects_local_order_below_min_notional() {
        let client = sample_client();
        client.symbol_rules_cache.write().await.insert(
            Symbol::BtcUsdc,
            SymbolTradingRules {
                price_filter: Some(PriceFilterRule {
                    min_price: dec("0.01"),
                    max_price: dec("1000000"),
                    tick_size: dec("0.01"),
                }),
                lot_size: Some(LotSizeRule {
                    min_qty: dec("0.00001"),
                    max_qty: dec("1000"),
                    step_size: dec("0.00001"),
                }),
                market_lot_size: None,
                min_notional: Some(MinNotionalRule {
                    min_notional: dec("10"),
                    apply_to_market: true,
                }),
            },
        );

        let prepared = client
            .prepare_order_request(OrderRequest {
                client_order_id: "bot-1".to_string(),
                symbol: Symbol::BtcUsdc,
                side: Side::Buy,
                order_type: OrderType::LimitMaker,
                price: Some(dec("60000.01")),
                quantity: dec("0.00009"),
                time_in_force: Some(TimeInForce::Gtc),
                post_only: true,
                reduce_only: false,
                source_intent_id: "intent-1".to_string(),
            })
            .await
            .unwrap();

        match prepared {
            crate::PreparedOrder::Rejected(report) => {
                assert_eq!(report.status, OrderStatus::Rejected);
                assert!(report
                    .message
                    .as_deref()
                    .unwrap_or_default()
                    .contains("effective local minNotional floor"));
            }
            other => panic!("unexpected prepared order: {other:?}"),
        }
    }

    #[tokio::test]
    async fn prepare_order_request_uses_local_min_notional_above_exchange_floor() {
        let client = sample_client();
        client.symbol_rules_cache.write().await.insert(
            Symbol::EthUsdc,
            SymbolTradingRules {
                price_filter: Some(PriceFilterRule {
                    min_price: dec("0.01"),
                    max_price: dec("1000000"),
                    tick_size: dec("0.01"),
                }),
                lot_size: Some(LotSizeRule {
                    min_qty: dec("0.0001"),
                    max_qty: dec("100000"),
                    step_size: dec("0.0001"),
                }),
                market_lot_size: None,
                min_notional: Some(MinNotionalRule {
                    min_notional: dec("5"),
                    apply_to_market: true,
                }),
            },
        );

        let prepared = client
            .prepare_order_request(OrderRequest {
                client_order_id: "bot-eth-1".to_string(),
                symbol: Symbol::EthUsdc,
                side: Side::Buy,
                order_type: OrderType::LimitMaker,
                price: Some(dec("2000.00")),
                quantity: dec("0.0040"),
                time_in_force: Some(TimeInForce::Gtc),
                post_only: true,
                reduce_only: false,
                source_intent_id: "intent-eth-1".to_string(),
            })
            .await
            .unwrap();

        match prepared {
            crate::PreparedOrder::Rejected(report) => {
                let message = report.message.as_deref().unwrap_or_default();
                assert!(message.contains("effective local minNotional floor"));
                assert!(message.contains("local_min_notional=10"));
                assert!(message.contains("exchange_min_notional=5"));
            }
            other => panic!("unexpected prepared order: {other:?}"),
        }
    }

    #[tokio::test]
    async fn naturally_convertible_user_stream_fill_remains_propagated() {
        let client = sample_client();
        let raw = execution_report_event_raw("USDC");

        let event = parse_user_stream_message(&raw).unwrap().unwrap();
        let event = client.enrich_user_stream_event(event).await.unwrap();
        match event {
            BinanceUserStreamEvent::Execution(execution) => {
                assert_eq!(execution.report.client_order_id, "bot-1");
                assert_eq!(execution.report.status, OrderStatus::PartiallyFilled);
                assert_eq!(
                    execution.fill.as_ref().and_then(|fill| fill.fee_quote),
                    Some(Decimal::from_str_exact("0.01").unwrap())
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn third_party_user_stream_fill_with_cached_conversion_is_enriched() {
        let client = sample_client();
        client.fee_quote_conversion_cache.write().await.insert(
            "BNBUSDC".to_string(),
            FeeQuoteConversionCacheEntry {
                price: Decimal::from(600u32),
                observed_at: now_utc(),
            },
        );
        let raw = execution_report_event_raw("BNB");

        let event = parse_user_stream_message(&raw).unwrap().unwrap();
        let event = client.enrich_user_stream_event(event).await.unwrap();
        match event {
            BinanceUserStreamEvent::Execution(execution) => {
                assert_eq!(
                    execution.fill.as_ref().and_then(|fill| fill.fee_quote),
                    Some(Decimal::from(6u32))
                );
                assert!(execution.fill_recovery.is_none());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn marks_unenriched_third_party_fill_for_recovery_instead_of_forwarding_it_raw() {
        let client = sample_client();
        let raw = execution_report_event_raw("BNB");
        let event = parse_user_stream_message(&raw).unwrap().unwrap();
        let error = anyhow!("BNBUSDC price unavailable");

        let stripped = client.mark_fill_recovery_after_enrichment_failure(event, &error);

        match stripped {
            BinanceUserStreamEvent::Execution(execution) => {
                assert!(execution.fill.is_none());
                assert_eq!(execution.report.client_order_id, "bot-1");
                let recovery = execution.fill_recovery.expect("fill recovery should be queued");
                assert_eq!(recovery.symbol, Symbol::BtcUsdc);
                assert_eq!(recovery.trade_id, "7");
                assert_eq!(recovery.order_id, "42");
                assert_eq!(recovery.fee_asset, "BNB");
                assert!(execution
                    .report
                    .message
                    .as_deref()
                    .unwrap_or_default()
                    .contains("fill queued for REST recovery after fee quote enrichment failure"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
