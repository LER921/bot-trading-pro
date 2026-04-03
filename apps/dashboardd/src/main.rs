use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use common::Decimal;
use config_loader::load_from_dir;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Pool, Row, Sqlite};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use time::OffsetDateTime;
use tokio::task;
use tracing::{error, info};

const DASHBOARD_HTML: &str = include_str!("dashboard.html");

#[derive(Clone)]
struct AppState {
    pool: Pool<Sqlite>,
    db_path: PathBuf,
    log_path: PathBuf,
    bind_address: String,
    read_only: bool,
}

#[derive(Debug)]
struct DashboardError(anyhow::Error);

impl From<anyhow::Error> for DashboardError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}

impl From<sqlx::Error> for DashboardError {
    fn from(error: sqlx::Error) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for DashboardError {
    fn into_response(self) -> Response {
        error!(error = %self.0, "dashboard request failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

type DashboardApiResult<T> = std::result::Result<T, DashboardError>;

#[derive(Debug, Clone, Serialize, Default)]
struct DashboardMeta {
    bind_address: String,
    read_only: bool,
    db_path: String,
    log_path: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct RuntimeTransitionView {
    transitioned_at: String,
    from_state: String,
    to_state: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct RuntimePanel {
    current_state: String,
    current_reason: String,
    entered_at: Option<String>,
    started_at: Option<String>,
    transitions: Vec<RuntimeTransitionView>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct HealthPanel {
    observed_at: String,
    overall_state: String,
    market_ws_state: String,
    market_ws_age_ms: Option<i64>,
    market_ws_reconnect_count: i64,
    user_ws_state: String,
    user_ws_age_ms: Option<i64>,
    user_ws_reconnect_count: i64,
    rest_state: String,
    rest_consecutive_failures: i64,
    rest_roundtrip_ms: Option<i64>,
    market_data_state: String,
    account_events_state: String,
    clock_drift_state: String,
    clock_drift_ms: i64,
    fallback_active: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
struct PnlSnapshotView {
    updated_at: String,
    realized_pnl_quote: String,
    unrealized_pnl_quote: String,
    net_pnl_quote: String,
    fees_quote: String,
    daily_pnl_quote: String,
    peak_net_pnl_quote: String,
    drawdown_quote: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct PnlHistoryPoint {
    updated_at: String,
    net_pnl_quote: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct SymbolPnlView {
    symbol: String,
    position_base: String,
    average_entry_price: Option<String>,
    mark_price: Option<String>,
    realized_pnl_quote: String,
    unrealized_pnl_quote: String,
    net_pnl_quote: String,
    fees_quote: String,
    peak_net_pnl_quote: String,
    drawdown_quote: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct WinRateView {
    wins: u64,
    losses: u64,
    realized_events: u64,
    realized_close_win_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct PnlPanel {
    latest: Option<PnlSnapshotView>,
    history: Vec<PnlHistoryPoint>,
    by_symbol: Vec<SymbolPnlView>,
    win_rate: WinRateView,
}

#[derive(Debug, Clone, Serialize, Default)]
struct AccountBalanceView {
    asset: String,
    updated_at: String,
    free: String,
    locked: String,
    total: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct AccountPanel {
    usdc: Option<AccountBalanceView>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct InventoryView {
    symbol: String,
    observed_at: String,
    base_position: String,
    quote_position: String,
    average_entry_price: Option<String>,
    mark_price: Option<String>,
    exposure_quote: Option<f64>,
    reserved_quote_notional: String,
    max_quote_notional: String,
    soft_inventory_base: String,
    max_inventory_base: String,
    neutralization_clip_fraction: String,
    reduce_only_trigger_ratio: String,
    neutralization_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct ExecutionStatsView {
    updated_at: String,
    total_submitted: u64,
    total_rejected: u64,
    total_local_validation_rejects: u64,
    total_benign_exchange_rejected: u64,
    risk_scored_rejections: u64,
    total_canceled: u64,
    total_manual_cancels: u64,
    total_external_cancels: u64,
    total_filled_reports: u64,
    total_stale_cancels: u64,
    total_duplicate_intents: u64,
    total_equivalent_order_skips: u64,
    reject_rate: String,
    risk_reject_rate: String,
    avg_fill_ratio: String,
    avg_slippage_bps: String,
    avg_decision_latency_ms: String,
    avg_submit_ack_latency_ms: String,
    avg_submit_to_first_report_ms: String,
    avg_submit_to_fill_ms: String,
    avg_cancel_ack_latency_ms: String,
    decision_latency_samples: u64,
    submit_ack_latency_samples: u64,
    submit_to_first_report_samples: u64,
    submit_to_fill_samples: u64,
    cancel_ack_latency_samples: u64,
    fill_report_rate: Option<f64>,
    cancel_rate: Option<f64>,
    stale_cancel_share: Option<f64>,
    duplicate_suppressions: u64,
    churn_state: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct ExecutionReportView {
    event_time: String,
    client_order_id: String,
    exchange_order_id: Option<String>,
    symbol: String,
    origin: String,
    classification: String,
    status: String,
    filled_quantity: String,
    average_fill_price: Option<String>,
    fill_ratio: String,
    requested_price: Option<String>,
    slippage_bps: Option<String>,
    decision_latency_ms: Option<i64>,
    submit_ack_latency_ms: Option<i64>,
    submit_to_first_report_ms: Option<i64>,
    submit_to_fill_ms: Option<i64>,
    exchange_order_age_ms: Option<i64>,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct StatusCountView {
    status: String,
    count: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct FillView {
    event_time: String,
    trade_id: String,
    order_id: String,
    symbol: String,
    side: String,
    origin: String,
    price: String,
    quantity: String,
    fee: String,
    fee_asset: String,
    fee_quote: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct ExecutionPanel {
    latest_stats: Option<ExecutionStatsView>,
    recent_reports: Vec<ExecutionReportView>,
    open_orders: Vec<ExecutionReportView>,
    recent_fills: Vec<FillView>,
    status_counts: Vec<StatusCountView>,
    local_reject_causes: Vec<StatusCountView>,
    total_fills: u64,
    external_fills: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct RiskDecisionView {
    decided_at: String,
    action: String,
    symbol: String,
    resulting_mode: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct RiskPanel {
    recent: Vec<RiskDecisionView>,
    recent_blocks: Vec<RiskDecisionView>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct StrategyOutcomeView {
    observed_at: String,
    symbol: String,
    strategy: String,
    status: String,
    standby_reason: String,
    intent_count: u64,
    best_edge_after_cost_bps: String,
    reduce_only_intents: u64,
    sample_reason: String,
    runtime_state: String,
    risk_mode: String,
    inventory_base: String,
    spread_bps: String,
    toxicity_score: String,
    local_momentum_bps: String,
    trade_flow_imbalance: String,
    orderbook_imbalance: Option<String>,
    low_edge_intents: u64,
    medium_edge_intents: u64,
    high_edge_intents: u64,
    reduce_risk_intents: u64,
    add_risk_intents: u64,
    open_order_slots_used: u64,
    max_open_order_slots: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct StrategyPanel {
    latest_by_symbol: Vec<StrategyOutcomeView>,
    recent: Vec<StrategyOutcomeView>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct LogEntry {
    timestamp: String,
    level: String,
    target: String,
    message: String,
    details: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct LogsPanel {
    runtime: Vec<LogEntry>,
    health: Vec<LogEntry>,
    strategy: Vec<LogEntry>,
    risk: Vec<LogEntry>,
    execution: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct DashboardSummary {
    generated_at: String,
    meta: DashboardMeta,
    runtime: RuntimePanel,
    health: Option<HealthPanel>,
    account: AccountPanel,
    pnl: PnlPanel,
    inventory: Vec<InventoryView>,
    execution: ExecutionPanel,
    risk: RiskPanel,
    strategy: StrategyPanel,
    logs: LogsPanel,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = load_from_dir("config")?;
    let db_path = PathBuf::from(&config.runtime.state_dir).join("traderd.sqlite3");
    let log_path = PathBuf::from(&config.runtime.event_log_dir).join("traderd-live.log");

    let connect_options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))?
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .context("failed to open traderd sqlite database for dashboard")?;
    ensure_dashboard_schema(&pool).await?;

    let state = AppState {
        pool,
        db_path: db_path.clone(),
        log_path: log_path.clone(),
        bind_address: config.dashboard.bind_address.clone(),
        read_only: !config.dashboard.enable_write_routes,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/runtime", get(runtime))
        .route("/api/summary", get(summary))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&config.dashboard.bind_address)
        .await
        .with_context(|| format!("failed to bind dashboard to {}", config.dashboard.bind_address))?;
    info!(
        bind_address = %config.dashboard.bind_address,
        db_path = %db_path.display(),
        log_path = %log_path.display(),
        "dashboardd started"
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn runtime(State(state): State<AppState>) -> DashboardApiResult<Json<RuntimePanel>> {
    let summary = build_summary(&state).await?;
    Ok(Json(summary.runtime))
}

async fn summary(State(state): State<AppState>) -> DashboardApiResult<Json<DashboardSummary>> {
    Ok(Json(build_summary(&state).await?))
}

async fn build_summary(state: &AppState) -> Result<DashboardSummary> {
    let runtime = fetch_runtime_panel(&state.pool).await?;
    let health = fetch_health_panel(&state.pool).await?;
    let session_started_at = runtime.started_at.as_deref();
    let account = fetch_account_panel(&state.pool).await?;
    let pnl_latest = fetch_latest_pnl_snapshot(&state.pool, session_started_at).await?;
    let pnl_history = fetch_pnl_history(&state.pool, session_started_at).await?;
    let pnl_by_symbol = fetch_symbol_pnl(&state.pool, session_started_at).await?;
    let win_rate = fetch_win_rate(&state.pool, session_started_at).await?;
    let inventory = fetch_inventory(&state.pool).await?;
    let latest_stats = fetch_execution_stats(&state.pool).await?;
    let recent_reports = fetch_recent_execution_reports(&state.pool, session_started_at).await?;
    let open_orders = fetch_open_orders(&state.pool).await?;
    let recent_fills = fetch_recent_fills(&state.pool, session_started_at).await?;
    let status_counts = fetch_execution_status_counts(&state.pool, session_started_at).await?;
    let local_reject_causes = fetch_local_reject_causes(&state.pool, session_started_at).await?;
    let (total_fills, external_fills) = fetch_fill_counts(&state.pool, session_started_at).await?;
    let recent_risk = fetch_recent_risk_decisions(&state.pool).await?;
    let recent_blocks = recent_risk
        .iter()
        .filter(|decision| decision.action.eq_ignore_ascii_case("Block"))
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    let latest_strategy = fetch_latest_strategy_by_symbol(&state.pool).await?;
    let recent_strategy = fetch_recent_strategy_outcomes(&state.pool).await?;
    let logs = fetch_logs(&state.log_path).await?;

    Ok(DashboardSummary {
        generated_at: OffsetDateTime::now_utc().to_string(),
        meta: DashboardMeta {
            bind_address: state.bind_address.clone(),
            read_only: state.read_only,
            db_path: state.db_path.display().to_string(),
            log_path: state.log_path.display().to_string(),
        },
        runtime,
        health,
        account,
        pnl: PnlPanel {
            latest: pnl_latest,
            history: pnl_history,
            by_symbol: pnl_by_symbol,
            win_rate,
        },
        inventory,
        execution: ExecutionPanel {
            latest_stats,
            recent_reports,
            open_orders,
            recent_fills,
            status_counts,
            local_reject_causes,
            total_fills,
            external_fills,
        },
        risk: RiskPanel {
            recent: recent_risk,
            recent_blocks,
        },
        strategy: StrategyPanel {
            latest_by_symbol: latest_strategy,
            recent: recent_strategy,
        },
        logs,
    })
}

async fn fetch_runtime_panel(pool: &Pool<Sqlite>) -> Result<RuntimePanel> {
    let current = default_if_missing(
        sqlx::query(
            "SELECT transitioned_at, from_state, to_state, reason FROM runtime_transitions ORDER BY id DESC LIMIT 1",
        )
        .fetch_optional(pool)
        .await,
    )?;
    let transitions = default_if_missing(
        sqlx::query(
            "SELECT transitioned_at, from_state, to_state, reason FROM runtime_transitions ORDER BY id DESC LIMIT 20",
        )
        .fetch_all(pool)
        .await,
    )?;
    let started_at = default_if_missing(
        sqlx::query(
            r#"
            SELECT COALESCE(
                (
                    SELECT transitioned_at
                    FROM runtime_transitions
                    WHERE to_state = 'Bootstrap'
                    ORDER BY id DESC
                    LIMIT 1
                ),
                (
                    SELECT transitioned_at
                    FROM runtime_transitions
                    ORDER BY id ASC
                    LIMIT 1
                )
            ) AS started_at
            "#,
        )
        .fetch_optional(pool)
        .await,
    )?;

    Ok(RuntimePanel {
        current_state: current
            .as_ref()
            .map(|row| row.get::<String, _>("to_state"))
            .unwrap_or_else(|| "Unavailable".to_string()),
        current_reason: current
            .as_ref()
            .map(|row| row.get::<String, _>("reason"))
            .unwrap_or_else(|| "runtime has not persisted any transition yet".to_string()),
        entered_at: current
            .as_ref()
            .map(|row| row.get::<String, _>("transitioned_at")),
        started_at: started_at.and_then(|row| row.get::<Option<String>, _>("started_at")),
        transitions: transitions
            .into_iter()
            .map(|row| RuntimeTransitionView {
                transitioned_at: row.get("transitioned_at"),
                from_state: row.get("from_state"),
                to_state: row.get("to_state"),
                reason: row.get("reason"),
            })
            .collect(),
    })
}

async fn fetch_health_panel(pool: &Pool<Sqlite>) -> Result<Option<HealthPanel>> {
    let row = default_if_missing(
        sqlx::query(
            r#"
            SELECT observed_at, overall_state, market_ws_state, market_ws_age_ms,
                   market_ws_reconnect_count, user_ws_state, user_ws_age_ms,
                   user_ws_reconnect_count, rest_state, rest_consecutive_failures,
                   rest_roundtrip_ms,
                   market_data_state, account_events_state, clock_drift_state,
                   clock_drift_ms, fallback_active
            FROM health_snapshots
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(pool)
        .await,
    )?;

    Ok(row.map(|row| HealthPanel {
        observed_at: row.get("observed_at"),
        overall_state: row.get("overall_state"),
        market_ws_state: row.get("market_ws_state"),
        market_ws_age_ms: row.get("market_ws_age_ms"),
        market_ws_reconnect_count: row.get("market_ws_reconnect_count"),
        user_ws_state: row.get("user_ws_state"),
        user_ws_age_ms: row.get("user_ws_age_ms"),
        user_ws_reconnect_count: row.get("user_ws_reconnect_count"),
        rest_state: row.get("rest_state"),
        rest_consecutive_failures: row.get("rest_consecutive_failures"),
        rest_roundtrip_ms: row.get("rest_roundtrip_ms"),
        market_data_state: row.get("market_data_state"),
        account_events_state: row.get("account_events_state"),
        clock_drift_state: row.get("clock_drift_state"),
        clock_drift_ms: row.get("clock_drift_ms"),
        fallback_active: row.get::<i64, _>("fallback_active") != 0,
    }))
}

async fn fetch_account_panel(pool: &Pool<Sqlite>) -> Result<AccountPanel> {
    let usdc = default_if_missing(
        sqlx::query(
            r#"
            WITH latest AS (
                SELECT asset, MAX(id) AS max_id
                FROM account_snapshots
                GROUP BY asset
            )
            SELECT a.updated_at, a.asset, a.free, a.locked
            FROM account_snapshots a
            JOIN latest l ON l.max_id = a.id
            WHERE a.asset = 'USDC'
            LIMIT 1
            "#,
        )
        .fetch_optional(pool)
        .await,
    )?;

    Ok(AccountPanel {
        usdc: usdc.map(|row| {
            let free = row.get::<String, _>("free");
            let locked = row.get::<String, _>("locked");
            let total = add_decimal_strings(&free, &locked).unwrap_or_else(|| free.clone());
            AccountBalanceView {
                asset: row.get("asset"),
                updated_at: row.get("updated_at"),
                free,
                locked,
                total,
            }
        }),
    })
}

async fn fetch_latest_pnl_snapshot(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<Option<PnlSnapshotView>> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                r#"
                SELECT updated_at, realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote,
                       fees_quote, daily_pnl_quote, peak_net_pnl_quote, drawdown_quote
                FROM pnl_snapshots
                WHERE updated_at >= ?
                ORDER BY id ASC
                "#,
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT updated_at, realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote,
                       fees_quote, daily_pnl_quote, peak_net_pnl_quote, drawdown_quote
                FROM pnl_snapshots
                ORDER BY id ASC
                "#,
            )
            .fetch_all(pool)
            .await
        },
    )?;

    let Some(first_row) = rows.first() else {
        return Ok(None);
    };
    let latest_row = rows.last().unwrap_or(first_row);
    let baseline = map_pnl_snapshot_row(first_row);
    let latest = map_pnl_snapshot_row(latest_row);
    let net_points = rows
        .iter()
        .map(|row| {
            subtract_decimal_values(
                parse_decimal_string(&row.get::<String, _>("net_pnl_quote")).unwrap_or_default(),
                parse_decimal_string(&baseline.net_pnl_quote).unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();

    Ok(Some(PnlSnapshotView {
        updated_at: latest.updated_at,
        realized_pnl_quote: subtract_decimal_strings(
            &latest.realized_pnl_quote,
            &baseline.realized_pnl_quote,
        )
        .unwrap_or(latest.realized_pnl_quote),
        unrealized_pnl_quote: subtract_decimal_strings(
            &latest.unrealized_pnl_quote,
            &baseline.unrealized_pnl_quote,
        )
        .unwrap_or(latest.unrealized_pnl_quote),
        net_pnl_quote: subtract_decimal_strings(&latest.net_pnl_quote, &baseline.net_pnl_quote)
            .unwrap_or(latest.net_pnl_quote),
        fees_quote: subtract_decimal_strings(&latest.fees_quote, &baseline.fees_quote)
            .unwrap_or(latest.fees_quote),
        daily_pnl_quote: subtract_decimal_strings(
            &latest.daily_pnl_quote,
            &baseline.daily_pnl_quote,
        )
        .unwrap_or(latest.daily_pnl_quote),
        peak_net_pnl_quote: decimal_to_string(compute_peak_from_values(&net_points)),
        drawdown_quote: decimal_to_string(compute_drawdown_from_values(&net_points)),
    }))
}

async fn fetch_pnl_history(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<Vec<PnlHistoryPoint>> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                "SELECT updated_at, net_pnl_quote FROM pnl_snapshots WHERE updated_at >= ? ORDER BY id ASC",
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query("SELECT updated_at, net_pnl_quote FROM pnl_snapshots ORDER BY id ASC")
                .fetch_all(pool)
                .await
        },
    )?;
    let Some(first_row) = rows.first() else {
        return Ok(Vec::new());
    };
    let baseline_net =
        parse_decimal_string(&first_row.get::<String, _>("net_pnl_quote")).unwrap_or_default();
    let mut points = rows
        .into_iter()
        .map(|row| PnlHistoryPoint {
            updated_at: row.get("updated_at"),
            net_pnl_quote: parse_f64(&decimal_to_string(subtract_decimal_values(
                parse_decimal_string(&row.get::<String, _>("net_pnl_quote")).unwrap_or_default(),
                baseline_net,
            )))
            .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    if points.len() > 80 {
        points = points.split_off(points.len() - 80);
    }
    Ok(points)
}

async fn fetch_symbol_pnl(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<Vec<SymbolPnlView>> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                r#"
                SELECT updated_at, symbol, position_base, average_entry_price, mark_price,
                       realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote, fees_quote,
                       peak_net_pnl_quote, drawdown_quote
                FROM pnl_symbol_snapshots
                WHERE updated_at >= ?
                ORDER BY symbol ASC, id ASC
                "#,
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT updated_at, symbol, position_base, average_entry_price, mark_price,
                       realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote, fees_quote,
                       peak_net_pnl_quote, drawdown_quote
                FROM pnl_symbol_snapshots
                ORDER BY symbol ASC, id ASC
                "#,
            )
            .fetch_all(pool)
            .await
        },
    )?;

    let mut by_symbol = HashMap::<String, Vec<SymbolPnlView>>::new();
    for row in rows {
        let sample = map_symbol_pnl_snapshot_row(&row);
        by_symbol
            .entry(sample.symbol.clone())
            .or_default()
            .push(sample);
    }

    let mut snapshots = by_symbol
        .into_iter()
        .filter_map(|(_, samples)| {
            let baseline = samples.first()?;
            let latest = samples.last()?;
            let baseline_net =
                parse_decimal_string(&baseline.net_pnl_quote).unwrap_or_default();
            let net_points = samples
                .iter()
                .map(|sample| {
                    subtract_decimal_values(
                        parse_decimal_string(&sample.net_pnl_quote).unwrap_or_default(),
                        baseline_net,
                    )
                })
                .collect::<Vec<_>>();

            Some(SymbolPnlView {
                symbol: latest.symbol.clone(),
                position_base: latest.position_base.clone(),
                average_entry_price: latest.average_entry_price.clone(),
                mark_price: latest.mark_price.clone(),
                realized_pnl_quote: subtract_decimal_strings(
                    &latest.realized_pnl_quote,
                    &baseline.realized_pnl_quote,
                )
                .unwrap_or_else(|| latest.realized_pnl_quote.clone()),
                unrealized_pnl_quote: subtract_decimal_strings(
                    &latest.unrealized_pnl_quote,
                    &baseline.unrealized_pnl_quote,
                )
                .unwrap_or_else(|| latest.unrealized_pnl_quote.clone()),
                net_pnl_quote: subtract_decimal_strings(
                    &latest.net_pnl_quote,
                    &baseline.net_pnl_quote,
                )
                .unwrap_or_else(|| latest.net_pnl_quote.clone()),
                fees_quote: subtract_decimal_strings(
                    &latest.fees_quote,
                    &baseline.fees_quote,
                )
                .unwrap_or_else(|| latest.fees_quote.clone()),
                peak_net_pnl_quote: decimal_to_string(compute_peak_from_values(&net_points)),
                drawdown_quote: decimal_to_string(compute_drawdown_from_values(&net_points)),
            })
        })
        .collect::<Vec<_>>();
    snapshots.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    Ok(snapshots)
}

async fn fetch_win_rate(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<WinRateView> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                "SELECT symbol, updated_at, realized_pnl_quote FROM pnl_symbol_snapshots WHERE updated_at >= ? ORDER BY symbol ASC, id ASC",
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                "SELECT symbol, updated_at, realized_pnl_quote FROM pnl_symbol_snapshots ORDER BY symbol ASC, id ASC",
            )
            .fetch_all(pool)
            .await
        },
    )?;

    let mut previous_realized = HashMap::<String, Decimal>::new();
    let mut wins = 0u64;
    let mut losses = 0u64;

    for row in rows {
        let symbol = row.get::<String, _>("symbol");
        let realized =
            parse_decimal_string(&row.get::<String, _>("realized_pnl_quote")).unwrap_or_default();
        if let Some(previous) = previous_realized.insert(symbol, realized) {
            let delta = realized - previous;
            if delta > Decimal::ZERO {
                wins += 1;
            } else if delta < Decimal::ZERO {
                losses += 1;
            }
        }
    }

    let realized_events = wins + losses;
    Ok(WinRateView {
        wins,
        losses,
        realized_events,
        realized_close_win_rate: if realized_events == 0 {
            None
        } else {
            Some((wins as f64 / realized_events as f64) * 100.0)
        },
    })
}

async fn fetch_inventory(pool: &Pool<Sqlite>) -> Result<Vec<InventoryView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            WITH latest AS (
                SELECT symbol, MAX(id) AS max_id
                FROM inventory_snapshots
                GROUP BY symbol
            )
            SELECT i.observed_at, i.symbol, i.base_position, i.quote_position, i.mark_price,
                   i.average_entry_price, i.max_quote_notional, i.reserved_quote_notional,
                   i.soft_inventory_base, i.max_inventory_base,
                   i.neutralization_clip_fraction, i.reduce_only_trigger_ratio
            FROM inventory_snapshots i
            JOIN latest l ON l.max_id = i.id
            ORDER BY i.symbol ASC
            "#,
        )
        .fetch_all(pool)
        .await,
    )?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let base_position = row.get::<String, _>("base_position");
            let quote_position = row.get::<String, _>("quote_position");
            let mark_price = row.get::<Option<String>, _>("mark_price");
            let max_quote_notional = row.get::<String, _>("max_quote_notional");
            let reserved_quote_notional = row.get::<String, _>("reserved_quote_notional");

            InventoryView {
                symbol: row.get("symbol"),
                observed_at: row.get("observed_at"),
                base_position: base_position.clone(),
                quote_position: quote_position.clone(),
                average_entry_price: row.get("average_entry_price"),
                mark_price: mark_price.clone(),
                exposure_quote: compute_exposure_quote(
                    &base_position,
                    mark_price.as_deref(),
                    &quote_position,
                ),
                reserved_quote_notional: reserved_quote_notional.clone(),
                max_quote_notional: max_quote_notional.clone(),
                soft_inventory_base: row.get("soft_inventory_base"),
                max_inventory_base: row.get("max_inventory_base"),
                neutralization_clip_fraction: row.get("neutralization_clip_fraction"),
                reduce_only_trigger_ratio: row.get("reduce_only_trigger_ratio"),
                neutralization_ratio: compute_ratio(
                    parse_f64(&reserved_quote_notional),
                    parse_f64(&max_quote_notional),
                ),
            }
        })
        .collect())
}

async fn fetch_execution_stats(pool: &Pool<Sqlite>) -> Result<Option<ExecutionStatsView>> {
    let row = default_if_missing(
        sqlx::query(
            r#"
            SELECT updated_at, total_submitted, total_rejected, total_local_validation_rejects,
                   total_benign_exchange_rejected, risk_scored_rejections, total_canceled,
                   total_manual_cancels, total_external_cancels, total_filled_reports,
                   total_stale_cancels, total_duplicate_intents, total_equivalent_order_skips,
                   reject_rate, risk_reject_rate, avg_fill_ratio, avg_slippage_bps,
                   avg_decision_latency_ms, avg_submit_ack_latency_ms,
                   avg_submit_to_first_report_ms, avg_submit_to_fill_ms,
                   avg_cancel_ack_latency_ms, decision_latency_samples,
                   submit_ack_latency_samples, submit_to_first_report_samples,
                   submit_to_fill_samples, cancel_ack_latency_samples
            FROM execution_stats_snapshots
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(pool)
        .await,
    )?;

    Ok(row.map(|row| {
        let total_submitted = row.get::<i64, _>("total_submitted").max(0) as u64;
        let total_rejected = row.get::<i64, _>("total_rejected").max(0) as u64;
        let total_local_validation_rejects =
            row.get::<i64, _>("total_local_validation_rejects").max(0) as u64;
        let total_benign_exchange_rejected =
            row.get::<i64, _>("total_benign_exchange_rejected").max(0) as u64;
        let risk_scored_rejections =
            row.get::<i64, _>("risk_scored_rejections").max(0) as u64;
        let total_canceled = row.get::<i64, _>("total_canceled").max(0) as u64;
        let total_filled_reports = row.get::<i64, _>("total_filled_reports").max(0) as u64;
        let total_stale_cancels = row.get::<i64, _>("total_stale_cancels").max(0) as u64;
        let total_duplicate_intents = row.get::<i64, _>("total_duplicate_intents").max(0) as u64;
        let total_equivalent_order_skips =
            row.get::<i64, _>("total_equivalent_order_skips").max(0) as u64;
        let fill_report_rate = compute_ratio(
            Some(total_filled_reports as f64),
            Some(total_submitted as f64),
        );
        let cancel_rate =
            compute_ratio(Some(total_canceled as f64), Some(total_submitted as f64));
        let stale_cancel_share = compute_ratio(
            Some(total_stale_cancels as f64),
            Some(total_canceled as f64),
        );

        ExecutionStatsView {
            updated_at: row.get("updated_at"),
            total_submitted,
            total_rejected,
            total_local_validation_rejects,
            total_benign_exchange_rejected,
            risk_scored_rejections,
            total_canceled,
            total_manual_cancels: row.get::<i64, _>("total_manual_cancels").max(0) as u64,
            total_external_cancels: row.get::<i64, _>("total_external_cancels").max(0) as u64,
            total_filled_reports,
            total_stale_cancels,
            total_duplicate_intents,
            total_equivalent_order_skips,
            reject_rate: row.get("reject_rate"),
            risk_reject_rate: row.get("risk_reject_rate"),
            avg_fill_ratio: row.get("avg_fill_ratio"),
            avg_slippage_bps: row.get("avg_slippage_bps"),
            avg_decision_latency_ms: row.get("avg_decision_latency_ms"),
            avg_submit_ack_latency_ms: row.get("avg_submit_ack_latency_ms"),
            avg_submit_to_first_report_ms: row.get("avg_submit_to_first_report_ms"),
            avg_submit_to_fill_ms: row.get("avg_submit_to_fill_ms"),
            avg_cancel_ack_latency_ms: row.get("avg_cancel_ack_latency_ms"),
            decision_latency_samples: row.get::<i64, _>("decision_latency_samples").max(0) as u64,
            submit_ack_latency_samples: row.get::<i64, _>("submit_ack_latency_samples").max(0)
                as u64,
            submit_to_first_report_samples: row
                .get::<i64, _>("submit_to_first_report_samples")
                .max(0) as u64,
            submit_to_fill_samples: row.get::<i64, _>("submit_to_fill_samples").max(0) as u64,
            cancel_ack_latency_samples: row.get::<i64, _>("cancel_ack_latency_samples").max(0)
                as u64,
            fill_report_rate,
            cancel_rate,
            stale_cancel_share,
            duplicate_suppressions: total_duplicate_intents + total_equivalent_order_skips,
            churn_state: churn_state(fill_report_rate, cancel_rate),
        }
    }))
}

async fn fetch_recent_execution_reports(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<Vec<ExecutionReportView>> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                r#"
                SELECT event_time, client_order_id, exchange_order_id, symbol, status,
                       filled_quantity, average_fill_price, fill_ratio, requested_price,
                       slippage_bps, decision_latency_ms, submit_ack_latency_ms,
                       submit_to_first_report_ms, submit_to_fill_ms, exchange_order_age_ms, message
                FROM execution_reports
                WHERE event_time >= ?
                ORDER BY id DESC
                LIMIT 25
                "#,
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT event_time, client_order_id, exchange_order_id, symbol, status,
                       filled_quantity, average_fill_price, fill_ratio, requested_price,
                       slippage_bps, decision_latency_ms, submit_ack_latency_ms,
                       submit_to_first_report_ms, submit_to_fill_ms, exchange_order_age_ms, message
                FROM execution_reports
                ORDER BY id DESC
                LIMIT 25
                "#,
            )
            .fetch_all(pool)
            .await
        },
    )?;
    Ok(rows.into_iter().map(map_execution_report).collect())
}

async fn fetch_open_orders(pool: &Pool<Sqlite>) -> Result<Vec<ExecutionReportView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            WITH latest AS (
                SELECT client_order_id, MAX(id) AS max_id
                FROM execution_reports
                GROUP BY client_order_id
            )
            SELECT e.event_time, e.client_order_id, e.exchange_order_id, e.symbol, e.status,
                   e.filled_quantity, e.average_fill_price, e.fill_ratio, e.requested_price,
                   e.slippage_bps, e.decision_latency_ms, e.submit_ack_latency_ms,
                   e.submit_to_first_report_ms, e.submit_to_fill_ms, e.exchange_order_age_ms, e.message
            FROM execution_reports e
            JOIN latest l ON l.max_id = e.id
            WHERE e.status IN ('New', 'PartiallyFilled')
            ORDER BY e.id DESC
            LIMIT 20
            "#,
        )
        .fetch_all(pool)
        .await,
    )?;
    Ok(rows.into_iter().map(map_execution_report).collect())
}

async fn fetch_recent_fills(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<Vec<FillView>> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                r#"
                WITH latest_reports AS (
                    SELECT exchange_order_id, MAX(id) AS max_id
                    FROM execution_reports
                    WHERE exchange_order_id IS NOT NULL
                    GROUP BY exchange_order_id
                )
                SELECT f.event_time, f.trade_id, f.order_id, f.symbol, f.side, f.price, f.quantity,
                       f.fee, f.fee_asset, f.fee_quote,
                       CASE
                           WHEN er.client_order_id LIKE 'bot-%' THEN 'bot'
                           ELSE 'external'
                       END AS origin
                FROM fills f
                LEFT JOIN latest_reports lr ON lr.exchange_order_id = f.order_id
                LEFT JOIN execution_reports er ON er.id = lr.max_id
                WHERE f.event_time >= ?
                ORDER BY f.id DESC
                LIMIT 40
                "#,
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                r#"
                WITH latest_reports AS (
                    SELECT exchange_order_id, MAX(id) AS max_id
                    FROM execution_reports
                    WHERE exchange_order_id IS NOT NULL
                    GROUP BY exchange_order_id
                )
                SELECT f.event_time, f.trade_id, f.order_id, f.symbol, f.side, f.price, f.quantity,
                       f.fee, f.fee_asset, f.fee_quote,
                       CASE
                           WHEN er.client_order_id LIKE 'bot-%' THEN 'bot'
                           ELSE 'external'
                       END AS origin
                FROM fills f
                LEFT JOIN latest_reports lr ON lr.exchange_order_id = f.order_id
                LEFT JOIN execution_reports er ON er.id = lr.max_id
                ORDER BY f.id DESC
                LIMIT 40
                "#,
            )
            .fetch_all(pool)
            .await
        },
    )?;
    Ok(rows
        .into_iter()
        .map(|row| FillView {
            event_time: row.get("event_time"),
            trade_id: row.get("trade_id"),
            order_id: row.get("order_id"),
            symbol: row.get("symbol"),
            side: row.get("side"),
            origin: row.get("origin"),
            price: row.get("price"),
            quantity: row.get("quantity"),
            fee: row.get("fee"),
            fee_asset: row.get("fee_asset"),
            fee_quote: row.get("fee_quote"),
        })
        .collect())
}

async fn fetch_fill_counts(pool: &Pool<Sqlite>, session_started_at: Option<&str>) -> Result<(u64, u64)> {
    let row = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                r#"
                WITH latest_reports AS (
                    SELECT exchange_order_id, MAX(id) AS max_id
                    FROM execution_reports
                    WHERE exchange_order_id IS NOT NULL
                    GROUP BY exchange_order_id
                ),
                classified AS (
                    SELECT CASE
                               WHEN er.client_order_id LIKE 'bot-%' THEN 'bot'
                               ELSE 'external'
                           END AS origin
                    FROM fills f
                    LEFT JOIN latest_reports lr ON lr.exchange_order_id = f.order_id
                    LEFT JOIN execution_reports er ON er.id = lr.max_id
                    WHERE f.event_time >= ?
                )
                SELECT SUM(CASE WHEN origin = 'bot' THEN 1 ELSE 0 END) AS bot_count,
                       SUM(CASE WHEN origin = 'external' THEN 1 ELSE 0 END) AS external_count
                FROM classified
                "#,
            )
                .bind(since)
                .fetch_optional(pool)
                .await
        } else {
            sqlx::query(
                r#"
                WITH latest_reports AS (
                    SELECT exchange_order_id, MAX(id) AS max_id
                    FROM execution_reports
                    WHERE exchange_order_id IS NOT NULL
                    GROUP BY exchange_order_id
                ),
                classified AS (
                    SELECT CASE
                               WHEN er.client_order_id LIKE 'bot-%' THEN 'bot'
                               ELSE 'external'
                           END AS origin
                    FROM fills f
                    LEFT JOIN latest_reports lr ON lr.exchange_order_id = f.order_id
                    LEFT JOIN execution_reports er ON er.id = lr.max_id
                )
                SELECT SUM(CASE WHEN origin = 'bot' THEN 1 ELSE 0 END) AS bot_count,
                       SUM(CASE WHEN origin = 'external' THEN 1 ELSE 0 END) AS external_count
                FROM classified
                "#,
            )
                .fetch_optional(pool)
                .await
        },
    )?;
    Ok(row
        .map(|row| {
            (
                row.get::<Option<i64>, _>("bot_count").unwrap_or_default().max(0) as u64,
                row.get::<Option<i64>, _>("external_count").unwrap_or_default().max(0) as u64,
            )
        })
        .unwrap_or_default())
}

async fn fetch_execution_status_counts(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<Vec<StatusCountView>> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                r#"
                WITH latest AS (
                    SELECT client_order_id, MAX(id) AS max_id
                    FROM execution_reports
                    WHERE event_time >= ?
                    GROUP BY client_order_id
                )
                SELECT e.status, COUNT(*) AS count
                FROM execution_reports e
                JOIN latest l ON l.max_id = e.id
                GROUP BY e.status
                ORDER BY count DESC, e.status ASC
                "#,
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                r#"
                WITH latest AS (
                    SELECT client_order_id, MAX(id) AS max_id
                    FROM execution_reports
                    GROUP BY client_order_id
                )
                SELECT e.status, COUNT(*) AS count
                FROM execution_reports e
                JOIN latest l ON l.max_id = e.id
                GROUP BY e.status
                ORDER BY count DESC, e.status ASC
                "#,
            )
            .fetch_all(pool)
            .await
        },
    )?;
    Ok(rows
        .into_iter()
        .map(|row| StatusCountView {
            status: row.get("status"),
            count: row.get::<i64, _>("count").max(0) as u64,
        })
        .collect())
}

async fn fetch_local_reject_causes(
    pool: &Pool<Sqlite>,
    session_started_at: Option<&str>,
) -> Result<Vec<StatusCountView>> {
    let rows = default_if_missing(
        if let Some(since) = session_started_at {
            sqlx::query(
                r#"
                SELECT message
                FROM execution_reports
                WHERE event_time >= ?
                  AND status = 'Rejected'
                  AND client_order_id LIKE 'bot-%'
                  AND message IS NOT NULL
                ORDER BY id DESC
                LIMIT 200
                "#,
            )
            .bind(since)
            .fetch_all(pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT message
                FROM execution_reports
                WHERE status = 'Rejected'
                  AND client_order_id LIKE 'bot-%'
                  AND message IS NOT NULL
                ORDER BY id DESC
                LIMIT 200
                "#,
            )
            .fetch_all(pool)
            .await
        },
    )?;

    let mut counts = HashMap::<String, u64>::new();
    for row in rows {
        let Some(message) = row.get::<Option<String>, _>("message") else {
            continue;
        };
        if !message
            .to_ascii_lowercase()
            .starts_with("local order rejection:")
        {
            continue;
        }
        *counts
            .entry(classify_local_reject_cause(&message).to_string())
            .or_default() += 1;
    }

    let mut items = counts
        .into_iter()
        .map(|(status, count)| StatusCountView { status, count })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| right.count.cmp(&left.count).then_with(|| left.status.cmp(&right.status)));
    Ok(items)
}

async fn fetch_recent_risk_decisions(pool: &Pool<Sqlite>) -> Result<Vec<RiskDecisionView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            SELECT decided_at, action, symbol, resulting_mode, reason
            FROM risk_decisions
            ORDER BY id DESC
            LIMIT 25
            "#,
        )
        .fetch_all(pool)
        .await,
    )?;
    Ok(rows
        .into_iter()
        .map(|row| RiskDecisionView {
            decided_at: row.get("decided_at"),
            action: row.get("action"),
            symbol: row.get("symbol"),
            resulting_mode: row.get("resulting_mode"),
            reason: row.get("reason"),
        })
        .collect())
}

async fn fetch_latest_strategy_by_symbol(pool: &Pool<Sqlite>) -> Result<Vec<StrategyOutcomeView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            WITH latest AS (
                SELECT symbol, MAX(id) AS max_id
                FROM strategy_outcomes
                GROUP BY symbol
            )
            SELECT s.observed_at, s.symbol, s.strategy, s.status, s.standby_reason, s.intent_count,
                   s.best_edge_after_cost_bps, s.reduce_only_intents, s.sample_reason,
                   s.runtime_state, s.risk_mode, s.inventory_base, s.spread_bps,
                   s.toxicity_score, s.local_momentum_bps, s.trade_flow_imbalance, s.orderbook_imbalance,
                   s.low_edge_intents, s.medium_edge_intents, s.high_edge_intents,
                   s.reduce_risk_intents, s.add_risk_intents, s.open_order_slots_used, s.max_open_order_slots
            FROM strategy_outcomes s
            JOIN latest l ON l.max_id = s.id
            ORDER BY s.symbol ASC
            "#,
        )
        .fetch_all(pool)
        .await,
    )?;
    Ok(rows.into_iter().map(map_strategy_outcome).collect())
}

async fn fetch_recent_strategy_outcomes(pool: &Pool<Sqlite>) -> Result<Vec<StrategyOutcomeView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            SELECT observed_at, symbol, strategy, status, standby_reason, intent_count,
                   best_edge_after_cost_bps, reduce_only_intents, sample_reason,
                   runtime_state, risk_mode, inventory_base, spread_bps,
                   toxicity_score, local_momentum_bps, trade_flow_imbalance, orderbook_imbalance,
                   low_edge_intents, medium_edge_intents, high_edge_intents,
                   reduce_risk_intents, add_risk_intents, open_order_slots_used, max_open_order_slots
            FROM strategy_outcomes
            ORDER BY id DESC
            LIMIT 25
            "#,
        )
        .fetch_all(pool)
        .await,
    )?;
    Ok(rows.into_iter().map(map_strategy_outcome).collect())
}

async fn fetch_logs(path: &Path) -> Result<LogsPanel> {
    let entries = tail_recent_logs(path.to_path_buf(), 400).await?;
    let mut panel = LogsPanel::default();

    for entry in entries.into_iter().rev() {
        match classify_log(&entry) {
            Some("runtime") if panel.runtime.len() < 20 => panel.runtime.push(entry),
            Some("health") if panel.health.len() < 20 => panel.health.push(entry),
            Some("strategy") if panel.strategy.len() < 20 => panel.strategy.push(entry),
            Some("risk") if panel.risk.len() < 20 => panel.risk.push(entry),
            Some("execution") if panel.execution.len() < 20 => panel.execution.push(entry),
            _ => {}
        }
    }

    Ok(panel)
}

async fn tail_recent_logs(path: PathBuf, max_lines: usize) -> Result<Vec<LogEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    task::spawn_blocking(move || {
        let file = File::open(&path)
            .with_context(|| format!("failed to open dashboard log source {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut buffer = VecDeque::with_capacity(max_lines);

        for line in reader.lines() {
            let line = line?;
            if buffer.len() == max_lines {
                buffer.pop_front();
            }
            buffer.push_back(line);
        }

        Ok::<Vec<LogEntry>, anyhow::Error>(
            buffer
                .into_iter()
                .filter_map(|line| parse_log_line(&line))
                .collect(),
        )
    })
    .await
    .context("failed to join log reader task")?
}

fn parse_log_line(line: &str) -> Option<LogEntry> {
    let value: Value = serde_json::from_str(line).ok()?;
    let fields = value.get("fields")?;
    let message = fields
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("log");

    Some(LogEntry {
        timestamp: value
            .get("timestamp")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        level: value
            .get("level")
            .and_then(Value::as_str)
            .unwrap_or("INFO")
            .to_string(),
        target: value
            .get("target")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        message: message.to_string(),
        details: fields_to_details(fields),
    })
}

fn fields_to_details(fields: &Value) -> String {
    let Some(object) = fields.as_object() else {
        return String::new();
    };

    object
        .iter()
        .filter(|(key, _)| *key != "message")
        .map(|(key, value)| format!("{key}={}", compact_value(value)))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn compact_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(string) => string.clone(),
        _ => value.to_string(),
    }
}

fn classify_log(entry: &LogEntry) -> Option<&'static str> {
    match entry.message.as_str() {
        "runtime state" | "runtime transition" | "stream lifecycle update" => Some("runtime"),
        "system health" => Some("health"),
        "strategy outcome" | "features" | "regime" => Some("strategy"),
        "risk decision" => Some("risk"),
        "execution report" | "execution stats" | "pnl snapshot" => Some("execution"),
        _ if entry.target.contains("traderd::runtime") => Some("runtime"),
        _ => None,
    }
}

fn map_execution_report(row: SqliteRow) -> ExecutionReportView {
    let client_order_id = row.get::<String, _>("client_order_id");
    let status = row.get::<String, _>("status");
    let message = row.get::<Option<String>, _>("message");
    ExecutionReportView {
        event_time: row.get("event_time"),
        client_order_id: client_order_id.clone(),
        exchange_order_id: row.get("exchange_order_id"),
        symbol: row.get("symbol"),
        origin: report_origin(&client_order_id).to_string(),
        classification: classify_execution_report(&status, message.as_deref()).to_string(),
        status,
        filled_quantity: row.get("filled_quantity"),
        average_fill_price: row.get("average_fill_price"),
        fill_ratio: row.get("fill_ratio"),
        requested_price: row.get("requested_price"),
        slippage_bps: row.get("slippage_bps"),
        decision_latency_ms: row.get("decision_latency_ms"),
        submit_ack_latency_ms: row.get("submit_ack_latency_ms"),
        submit_to_first_report_ms: row.get("submit_to_first_report_ms"),
        submit_to_fill_ms: row.get("submit_to_fill_ms"),
        exchange_order_age_ms: row.get("exchange_order_age_ms"),
        message,
    }
}

fn map_strategy_outcome(row: SqliteRow) -> StrategyOutcomeView {
    StrategyOutcomeView {
        observed_at: row.get("observed_at"),
        symbol: row.get("symbol"),
        strategy: row.get("strategy"),
        status: row.get("status"),
        standby_reason: row.get("standby_reason"),
        intent_count: row.get::<i64, _>("intent_count").max(0) as u64,
        best_edge_after_cost_bps: row.get("best_edge_after_cost_bps"),
        reduce_only_intents: row.get::<i64, _>("reduce_only_intents").max(0) as u64,
        sample_reason: row.get("sample_reason"),
        runtime_state: row.get("runtime_state"),
        risk_mode: row.get("risk_mode"),
        inventory_base: row.get("inventory_base"),
        spread_bps: row.get("spread_bps"),
        toxicity_score: row.get("toxicity_score"),
        local_momentum_bps: row.get("local_momentum_bps"),
        trade_flow_imbalance: row.get("trade_flow_imbalance"),
        orderbook_imbalance: row.get("orderbook_imbalance"),
        low_edge_intents: row.get::<i64, _>("low_edge_intents").max(0) as u64,
        medium_edge_intents: row.get::<i64, _>("medium_edge_intents").max(0) as u64,
        high_edge_intents: row.get::<i64, _>("high_edge_intents").max(0) as u64,
        reduce_risk_intents: row.get::<i64, _>("reduce_risk_intents").max(0) as u64,
        add_risk_intents: row.get::<i64, _>("add_risk_intents").max(0) as u64,
        open_order_slots_used: row.get::<i64, _>("open_order_slots_used").max(0) as u64,
        max_open_order_slots: row.get::<i64, _>("max_open_order_slots").max(0) as u64,
    }
}

fn map_pnl_snapshot_row(row: &SqliteRow) -> PnlSnapshotView {
    PnlSnapshotView {
        updated_at: row.get("updated_at"),
        realized_pnl_quote: row.get("realized_pnl_quote"),
        unrealized_pnl_quote: row.get("unrealized_pnl_quote"),
        net_pnl_quote: row.get("net_pnl_quote"),
        fees_quote: row.get("fees_quote"),
        daily_pnl_quote: row.get("daily_pnl_quote"),
        peak_net_pnl_quote: row.get("peak_net_pnl_quote"),
        drawdown_quote: row.get("drawdown_quote"),
    }
}

fn map_symbol_pnl_snapshot_row(row: &SqliteRow) -> SymbolPnlView {
    SymbolPnlView {
        symbol: row.get("symbol"),
        position_base: row.get("position_base"),
        average_entry_price: row.get("average_entry_price"),
        mark_price: row.get("mark_price"),
        realized_pnl_quote: row.get("realized_pnl_quote"),
        unrealized_pnl_quote: row.get("unrealized_pnl_quote"),
        net_pnl_quote: row.get("net_pnl_quote"),
        fees_quote: row.get("fees_quote"),
        peak_net_pnl_quote: row.get("peak_net_pnl_quote"),
        drawdown_quote: row.get("drawdown_quote"),
    }
}

fn compute_exposure_quote(
    base_position: &str,
    mark_price: Option<&str>,
    quote_position: &str,
) -> Option<f64> {
    let base = parse_f64(base_position)?;
    let quote = parse_f64(quote_position).unwrap_or_default().abs();
    let base_quote = mark_price
        .and_then(parse_f64)
        .map(|mark| (base * mark).abs())
        .unwrap_or_default();
    Some(base_quote.max(quote))
}

fn compute_ratio(numerator: Option<f64>, denominator: Option<f64>) -> Option<f64> {
    match (numerator, denominator) {
        (Some(numerator), Some(denominator)) if denominator.abs() > f64::EPSILON => {
            Some((numerator / denominator) * 100.0)
        }
        _ => None,
    }
}

fn churn_state(fill_report_rate: Option<f64>, cancel_rate: Option<f64>) -> String {
    match (fill_report_rate, cancel_rate) {
        (Some(fill_report_rate), Some(cancel_rate))
            if cancel_rate >= 75.0 && fill_report_rate <= 25.0 =>
        {
            "high_churn".to_string()
        }
        (Some(fill_report_rate), Some(cancel_rate))
            if fill_report_rate >= 50.0 && cancel_rate <= 35.0 =>
        {
            "efficient".to_string()
        }
        (Some(_), Some(_)) => "mixed".to_string(),
        _ => "idle".to_string(),
    }
}

fn report_origin(client_order_id: &str) -> &'static str {
    if client_order_id.starts_with("bot-") {
        "bot"
    } else {
        "external"
    }
}

fn classify_execution_report(status: &str, message: Option<&str>) -> &'static str {
    let normalized_status = status.to_ascii_lowercase();
    let normalized_message = message.unwrap_or_default().to_ascii_lowercase();
    if normalized_status == "rejected" {
        if normalized_message.starts_with("local order rejection:") {
            "local_validation_reject"
        } else {
            "exchange_reject"
        }
    } else if normalized_status == "canceled" {
        if normalized_message.contains("manual cancel") {
            "manual_cancel"
        } else if normalized_message.contains("stale") {
            "stale_cancel"
        } else {
            "technical_cancel"
        }
    } else {
        "execution_report"
    }
}

fn classify_local_reject_cause(message: &str) -> &'static str {
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("effective local minnotional floor") {
        "min_notional_floor"
    } else if normalized.contains("quantity collapsed") {
        "qty_collapsed"
    } else if normalized.contains("below lot_size minqty") {
        "below_min_qty"
    } else if normalized.contains("price must be > 0")
        || normalized.contains("requires a positive limit price")
    {
        "invalid_price"
    } else if normalized.contains("quantity must be > 0") {
        "invalid_quantity"
    } else if normalized.contains("price collapsed after tick normalization") {
        "tick_normalization"
    } else {
        "other_local"
    }
}

fn parse_f64(raw: &str) -> Option<f64> {
    raw.parse::<f64>().ok()
}

fn parse_decimal_string(raw: &str) -> Option<Decimal> {
    Decimal::from_str_exact(raw).ok()
}

fn decimal_to_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn add_decimal_strings(left: &str, right: &str) -> Option<String> {
    Some(decimal_to_string(
        parse_decimal_string(left)? + parse_decimal_string(right)?,
    ))
}

fn subtract_decimal_strings(left: &str, right: &str) -> Option<String> {
    Some(decimal_to_string(
        parse_decimal_string(left)? - parse_decimal_string(right)?,
    ))
}

fn subtract_decimal_values(left: Decimal, right: Decimal) -> Decimal {
    (left - right).normalize()
}

fn compute_peak_from_values(values: &[Decimal]) -> Decimal {
    values
        .iter()
        .copied()
        .max()
        .unwrap_or(Decimal::ZERO)
        .normalize()
}

fn compute_drawdown_from_values(values: &[Decimal]) -> Decimal {
    let mut peak = Decimal::ZERO;
    let mut drawdown = Decimal::ZERO;
    for value in values {
        if *value > peak {
            peak = *value;
        }
        let current_drawdown = peak - *value;
        if current_drawdown > drawdown {
            drawdown = current_drawdown;
        }
    }
    drawdown.normalize()
}

fn default_if_missing<T: Default>(result: std::result::Result<T, sqlx::Error>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if is_missing_schema_error(&error) => Ok(T::default()),
        Err(error) => Err(error.into()),
    }
}

async fn ensure_dashboard_schema(pool: &Pool<Sqlite>) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS runtime_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transitioned_at TEXT NOT NULL,
            from_state TEXT NOT NULL,
            to_state TEXT NOT NULL,
            reason TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS health_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at TEXT NOT NULL,
            overall_state TEXT NOT NULL,
            market_ws_state TEXT NOT NULL,
            market_ws_age_ms INTEGER,
            market_ws_reconnect_count INTEGER NOT NULL,
            user_ws_state TEXT NOT NULL,
            user_ws_age_ms INTEGER,
            user_ws_reconnect_count INTEGER NOT NULL,
            rest_state TEXT NOT NULL,
            rest_consecutive_failures INTEGER NOT NULL,
            market_data_state TEXT NOT NULL,
            account_events_state TEXT NOT NULL,
            clock_drift_state TEXT NOT NULL,
            clock_drift_ms INTEGER NOT NULL,
            fallback_active INTEGER NOT NULL,
            rest_roundtrip_ms INTEGER
        );
        "#,
    )
    .execute(pool)
    .await?;
    add_column_if_missing(pool, "health_snapshots", "rest_roundtrip_ms", "INTEGER").await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS account_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at TEXT NOT NULL,
            asset TEXT NOT NULL,
            free TEXT NOT NULL,
            locked TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS risk_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decided_at TEXT NOT NULL,
            action TEXT NOT NULL,
            symbol TEXT NOT NULL,
            resulting_mode TEXT NOT NULL,
            reason TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS execution_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT NOT NULL,
            client_order_id TEXT NOT NULL,
            exchange_order_id TEXT,
            symbol TEXT NOT NULL,
            status TEXT NOT NULL,
            filled_quantity TEXT NOT NULL,
            average_fill_price TEXT,
            fill_ratio TEXT NOT NULL,
            requested_price TEXT,
            slippage_bps TEXT,
            decision_latency_ms INTEGER,
            submit_ack_latency_ms INTEGER,
            submit_to_first_report_ms INTEGER,
            submit_to_fill_ms INTEGER,
            exchange_order_age_ms INTEGER
        );
        "#,
    )
    .execute(pool)
    .await?;
    add_column_if_missing(pool, "execution_reports", "message", "TEXT").await?;
    add_column_if_missing(pool, "execution_reports", "submit_ack_latency_ms", "INTEGER").await?;
    add_column_if_missing(
        pool,
        "execution_reports",
        "submit_to_first_report_ms",
        "INTEGER",
    )
    .await?;
    add_column_if_missing(pool, "execution_reports", "submit_to_fill_ms", "INTEGER").await?;
    add_column_if_missing(pool, "execution_reports", "exchange_order_age_ms", "INTEGER").await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS execution_stats_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at TEXT NOT NULL,
            total_submitted INTEGER NOT NULL,
            total_rejected INTEGER NOT NULL,
            total_local_validation_rejects INTEGER NOT NULL DEFAULT 0,
            total_benign_exchange_rejected INTEGER NOT NULL DEFAULT 0,
            risk_scored_rejections INTEGER NOT NULL DEFAULT 0,
            total_canceled INTEGER NOT NULL,
            total_manual_cancels INTEGER NOT NULL,
            total_external_cancels INTEGER NOT NULL DEFAULT 0,
            total_filled_reports INTEGER NOT NULL,
            total_stale_cancels INTEGER NOT NULL,
            total_duplicate_intents INTEGER NOT NULL,
            total_equivalent_order_skips INTEGER NOT NULL,
            reject_rate TEXT NOT NULL,
            risk_reject_rate TEXT NOT NULL DEFAULT '0',
            avg_fill_ratio TEXT NOT NULL,
            avg_slippage_bps TEXT NOT NULL,
            avg_decision_latency_ms TEXT NOT NULL,
            avg_submit_ack_latency_ms TEXT NOT NULL DEFAULT '0',
            avg_submit_to_first_report_ms TEXT NOT NULL DEFAULT '0',
            avg_submit_to_fill_ms TEXT NOT NULL DEFAULT '0',
            avg_cancel_ack_latency_ms TEXT NOT NULL DEFAULT '0',
            decision_latency_samples INTEGER NOT NULL DEFAULT 0,
            submit_ack_latency_samples INTEGER NOT NULL DEFAULT 0,
            submit_to_first_report_samples INTEGER NOT NULL DEFAULT 0,
            submit_to_fill_samples INTEGER NOT NULL DEFAULT 0,
            cancel_ack_latency_samples INTEGER NOT NULL DEFAULT 0
        );
        "#,
    )
    .execute(pool)
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "total_local_validation_rejects",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "total_benign_exchange_rejected",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "risk_scored_rejections",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "total_external_cancels",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "risk_reject_rate",
        "TEXT NOT NULL DEFAULT '0'",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "avg_submit_ack_latency_ms",
        "TEXT NOT NULL DEFAULT '0'",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "avg_submit_to_first_report_ms",
        "TEXT NOT NULL DEFAULT '0'",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "avg_submit_to_fill_ms",
        "TEXT NOT NULL DEFAULT '0'",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "avg_cancel_ack_latency_ms",
        "TEXT NOT NULL DEFAULT '0'",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "decision_latency_samples",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "submit_ack_latency_samples",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "submit_to_first_report_samples",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "submit_to_fill_samples",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "execution_stats_snapshots",
        "cancel_ack_latency_samples",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TEXT NOT NULL,
            trade_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price TEXT NOT NULL,
            quantity TEXT NOT NULL,
            fee TEXT NOT NULL,
            fee_asset TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;
    add_column_if_missing(pool, "fills", "fee_quote", "TEXT").await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS inventory_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            base_position TEXT NOT NULL,
            quote_position TEXT NOT NULL,
            mark_price TEXT,
            average_entry_price TEXT,
            max_quote_notional TEXT NOT NULL,
            reserved_quote_notional TEXT NOT NULL,
            soft_inventory_base TEXT NOT NULL,
            max_inventory_base TEXT NOT NULL,
            neutralization_clip_fraction TEXT NOT NULL,
            reduce_only_trigger_ratio TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS strategy_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            status TEXT NOT NULL,
            standby_reason TEXT NOT NULL,
            intent_count INTEGER NOT NULL,
            best_edge_after_cost_bps TEXT NOT NULL,
            reduce_only_intents INTEGER NOT NULL,
            sample_reason TEXT NOT NULL,
            runtime_state TEXT NOT NULL,
            risk_mode TEXT NOT NULL,
            inventory_base TEXT NOT NULL,
            spread_bps TEXT NOT NULL,
            toxicity_score TEXT NOT NULL,
            local_momentum_bps TEXT NOT NULL,
            trade_flow_imbalance TEXT NOT NULL,
            orderbook_imbalance TEXT,
            low_edge_intents INTEGER NOT NULL DEFAULT 0,
            medium_edge_intents INTEGER NOT NULL DEFAULT 0,
            high_edge_intents INTEGER NOT NULL DEFAULT 0,
            reduce_risk_intents INTEGER NOT NULL DEFAULT 0,
            add_risk_intents INTEGER NOT NULL DEFAULT 0,
            open_order_slots_used INTEGER NOT NULL DEFAULT 0,
            max_open_order_slots INTEGER NOT NULL DEFAULT 0
        );
        "#,
    )
    .execute(pool)
    .await?;
    add_column_if_missing(
        pool,
        "strategy_outcomes",
        "low_edge_intents",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "strategy_outcomes",
        "medium_edge_intents",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "strategy_outcomes",
        "high_edge_intents",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "strategy_outcomes",
        "reduce_risk_intents",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "strategy_outcomes",
        "add_risk_intents",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "strategy_outcomes",
        "open_order_slots_used",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    add_column_if_missing(
        pool,
        "strategy_outcomes",
        "max_open_order_slots",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS pnl_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at TEXT NOT NULL,
            realized_pnl_quote TEXT NOT NULL,
            unrealized_pnl_quote TEXT NOT NULL,
            net_pnl_quote TEXT NOT NULL,
            fees_quote TEXT NOT NULL,
            daily_pnl_quote TEXT NOT NULL,
            peak_net_pnl_quote TEXT NOT NULL,
            drawdown_quote TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS pnl_symbol_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            position_base TEXT NOT NULL,
            average_entry_price TEXT,
            mark_price TEXT,
            realized_pnl_quote TEXT NOT NULL,
            unrealized_pnl_quote TEXT NOT NULL,
            net_pnl_quote TEXT NOT NULL,
            fees_quote TEXT NOT NULL,
            peak_net_pnl_quote TEXT NOT NULL,
            drawdown_quote TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn add_column_if_missing(
    pool: &Pool<Sqlite>,
    table: &str,
    column: &str,
    column_type: &str,
) -> Result<()> {
    let statement = format!("ALTER TABLE {table} ADD COLUMN {column} {column_type};");
    if let Err(error) = sqlx::query(&statement).execute(pool).await {
        if !is_duplicate_column_error(&error, column) {
            return Err(error.into());
        }
    }
    Ok(())
}

fn is_duplicate_column_error(error: &sqlx::Error, column: &str) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains(&format!("duplicate column name: {}", column.to_ascii_lowercase()))
}

fn is_missing_schema_error(error: &sqlx::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("no such table") || text.contains("no such column")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;
    use time::OffsetDateTime;

    async fn test_pool() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite memory pool");
        ensure_dashboard_schema(&pool)
            .await
            .expect("dashboard schema");
        pool
    }

    #[test]
    fn parses_structured_log_message() {
        let line = r#"{"timestamp":"2026-04-03T10:00:00Z","level":"INFO","fields":{"message":"execution stats","total_submitted":12},"target":"telemetry::sink"}"#;
        let entry = parse_log_line(line).expect("log entry");
        assert_eq!(entry.message, "execution stats");
        assert!(entry.details.contains("total_submitted=12"));
    }

    #[test]
    fn computes_churn_state_from_rates() {
        assert_eq!(churn_state(Some(20.0), Some(90.0)), "high_churn");
        assert_eq!(churn_state(Some(60.0), Some(20.0)), "efficient");
        assert_eq!(churn_state(Some(30.0), Some(40.0)), "mixed");
    }

    #[test]
    fn classifies_local_reject_causes_for_operator_summary() {
        assert_eq!(
            classify_local_reject_cause(
                "local order rejection: notional below effective local minNotional floor"
            ),
            "min_notional_floor"
        );
        assert_eq!(
            classify_local_reject_cause(
                "local order rejection: normalized quantity below LOT_SIZE minQty"
            ),
            "below_min_qty"
        );
        assert_eq!(
            classify_local_reject_cause(
                "local order rejection: quantity collapsed after LOT_SIZE normalization"
            ),
            "qty_collapsed"
        );
    }

    #[test]
    fn embeds_dashboard_shell() {
        assert!(DASHBOARD_HTML.contains("bot-dashboard"));
        assert!(DASHBOARD_HTML.contains("/api/summary"));
    }

    #[tokio::test]
    async fn latest_strategy_query_reads_real_rows_without_sql_ambiguity() {
        let pool = test_pool().await;
        sqlx::query(
            r#"
            INSERT INTO strategy_outcomes (
                observed_at, symbol, strategy, status, standby_reason, intent_count,
                best_edge_after_cost_bps, reduce_only_intents, sample_reason,
                runtime_state, risk_mode, inventory_base, spread_bps, toxicity_score,
                local_momentum_bps, trade_flow_imbalance, orderbook_imbalance,
                low_edge_intents, medium_edge_intents, high_edge_intents,
                reduce_risk_intents, add_risk_intents, open_order_slots_used, max_open_order_slots
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(OffsetDateTime::UNIX_EPOCH.to_string())
        .bind("BTCUSDC")
        .bind("market_making")
        .bind("standby")
        .bind("edge too small")
        .bind(0_i64)
        .bind("0")
        .bind(0_i64)
        .bind("edge too small")
        .bind("Paused")
        .bind("RiskOff")
        .bind("0.010")
        .bind("1.20")
        .bind("0.25")
        .bind("0.10")
        .bind("0.05")
        .bind(Some("0.33"))
        .bind(1_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(1_i64)
        .bind(4_i64)
        .execute(&pool)
        .await
        .expect("insert strategy outcome");

        let rows = fetch_latest_strategy_by_symbol(&pool)
            .await
            .expect("latest strategy rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "BTCUSDC");
        assert_eq!(rows[0].strategy, "market_making");
    }

    #[tokio::test]
    async fn summary_endpoint_tolerates_empty_schema() {
        let pool = test_pool().await;
        let temp_dir = std::env::temp_dir().join(format!(
            "dashboardd-test-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).expect("temp dir");
        let state = AppState {
            pool,
            db_path: temp_dir.join("traderd.sqlite3"),
            log_path: temp_dir.join("traderd-live.log"),
            bind_address: "127.0.0.1:8080".to_string(),
            read_only: true,
        };

        let summary = build_summary(&state).await.expect("dashboard summary");

        assert_eq!(summary.runtime.current_state, "Unavailable");
        assert!(summary.strategy.latest_by_symbol.is_empty());
        assert!(summary.execution.recent_reports.is_empty());
    }

    #[tokio::test]
    async fn summary_uses_latest_runtime_bootstrap_as_session_baseline() {
        let pool = test_pool().await;
        let old_bootstrap = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1);
        let session_bootstrap = OffsetDateTime::UNIX_EPOCH + time::Duration::hours(5);

        sqlx::query(
            "INSERT INTO runtime_transitions (transitioned_at, from_state, to_state, reason) VALUES (?, ?, ?, ?)",
        )
        .bind(old_bootstrap.to_string())
        .bind("Shutdown")
        .bind("Bootstrap")
        .bind("old boot")
        .execute(&pool)
        .await
        .expect("insert old bootstrap");
        sqlx::query(
            "INSERT INTO runtime_transitions (transitioned_at, from_state, to_state, reason) VALUES (?, ?, ?, ?)",
        )
        .bind((session_bootstrap + time::Duration::seconds(1)).to_string())
        .bind("Shutdown")
        .bind("Bootstrap")
        .bind("current boot")
        .execute(&pool)
        .await
        .expect("insert current bootstrap");
        sqlx::query(
            "INSERT INTO runtime_transitions (transitioned_at, from_state, to_state, reason) VALUES (?, ?, ?, ?)",
        )
        .bind((session_bootstrap + time::Duration::seconds(2)).to_string())
        .bind("Ready")
        .bind("Trading")
        .bind("live")
        .execute(&pool)
        .await
        .expect("insert runtime trading");

        sqlx::query(
            "INSERT INTO pnl_snapshots (updated_at, realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote, fees_quote, daily_pnl_quote, peak_net_pnl_quote, drawdown_quote) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind((old_bootstrap + time::Duration::seconds(10)).to_string())
        .bind("-12.70")
        .bind("0")
        .bind("-20.98")
        .bind("8.27")
        .bind("0")
        .bind("0")
        .bind("20.98")
        .execute(&pool)
        .await
        .expect("insert old pnl snapshot");
        sqlx::query(
            "INSERT INTO pnl_snapshots (updated_at, realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote, fees_quote, daily_pnl_quote, peak_net_pnl_quote, drawdown_quote) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind((session_bootstrap + time::Duration::seconds(10)).to_string())
        .bind("-12.70")
        .bind("0")
        .bind("-20.98")
        .bind("8.27")
        .bind("0")
        .bind("0")
        .bind("20.98")
        .execute(&pool)
        .await
        .expect("insert session baseline pnl snapshot");

        sqlx::query(
            "INSERT INTO pnl_symbol_snapshots (updated_at, symbol, position_base, average_entry_price, mark_price, realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote, fees_quote, peak_net_pnl_quote, drawdown_quote) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind((session_bootstrap + time::Duration::seconds(10)).to_string())
        .bind("BTCUSDC")
        .bind("0")
        .bind(None::<String>)
        .bind(Some("65000"))
        .bind("-9.78")
        .bind("0")
        .bind("-15.11")
        .bind("5.32")
        .bind("0")
        .bind("15.11")
        .execute(&pool)
        .await
        .expect("insert session symbol pnl");

        sqlx::query(
            "INSERT INTO fills (event_time, trade_id, order_id, symbol, side, price, quantity, fee, fee_asset, fee_quote) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind((old_bootstrap + time::Duration::seconds(20)).to_string())
        .bind("old-fill")
        .bind("old-order")
        .bind("BTCUSDC")
        .bind("Buy")
        .bind("60000")
        .bind("0.01")
        .bind("0.1")
        .bind("USDC")
        .bind(Some("0.1"))
        .execute(&pool)
        .await
        .expect("insert old fill");

        sqlx::query(
            "INSERT INTO account_snapshots (updated_at, asset, free, locked) VALUES (?, ?, ?, ?)",
        )
        .bind((session_bootstrap + time::Duration::seconds(12)).to_string())
        .bind("USDC")
        .bind("1000")
        .bind("10")
        .execute(&pool)
        .await
        .expect("insert usdc balance");

        let temp_dir = std::env::temp_dir().join(format!(
            "dashboardd-test-summary-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::create_dir_all(&temp_dir).expect("temp dir");
        let state = AppState {
            pool,
            db_path: temp_dir.join("traderd.sqlite3"),
            log_path: temp_dir.join("traderd-live.log"),
            bind_address: "127.0.0.1:8080".to_string(),
            read_only: true,
        };

        let summary = build_summary(&state).await.expect("dashboard summary");

        assert_eq!(
            summary.runtime.started_at,
            Some((session_bootstrap + time::Duration::seconds(1)).to_string())
        );
        assert_eq!(summary.execution.total_fills, 0);
        assert_eq!(
            summary.pnl.latest.as_ref().map(|pnl| pnl.net_pnl_quote.as_str()),
            Some("0")
        );
        assert_eq!(
            summary.account.usdc.as_ref().map(|balance| balance.total.as_str()),
            Some("1010")
        );
    }

    #[tokio::test]
    async fn execution_stats_query_reads_separate_latency_columns() {
        let pool = test_pool().await;
        sqlx::query(
            r#"
            INSERT INTO execution_stats_snapshots (
                updated_at, total_submitted, total_rejected, total_local_validation_rejects,
                total_benign_exchange_rejected, risk_scored_rejections, total_canceled,
                total_manual_cancels, total_external_cancels, total_filled_reports,
                total_stale_cancels, total_duplicate_intents, total_equivalent_order_skips,
                reject_rate, risk_reject_rate, avg_fill_ratio, avg_slippage_bps,
                avg_decision_latency_ms, avg_submit_ack_latency_ms,
                avg_submit_to_first_report_ms, avg_submit_to_fill_ms,
                avg_cancel_ack_latency_ms, decision_latency_samples,
                submit_ack_latency_samples, submit_to_first_report_samples,
                submit_to_fill_samples, cancel_ack_latency_samples
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(OffsetDateTime::UNIX_EPOCH.to_string())
        .bind(12_i64)
        .bind(2_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind(4_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind(3_i64)
        .bind(2_i64)
        .bind(1_i64)
        .bind(2_i64)
        .bind("0.1667")
        .bind("0.0833")
        .bind("0.33")
        .bind("1.25")
        .bind("42")
        .bind("88")
        .bind("135")
        .bind("650")
        .bind("91")
        .bind(5_i64)
        .bind(4_i64)
        .bind(3_i64)
        .bind(2_i64)
        .bind(6_i64)
        .execute(&pool)
        .await
        .expect("insert execution stats");

        let stats = fetch_execution_stats(&pool)
            .await
            .expect("execution stats query")
            .expect("execution stats row");

        assert_eq!(stats.avg_decision_latency_ms, "42");
        assert_eq!(stats.avg_submit_ack_latency_ms, "88");
        assert_eq!(stats.avg_submit_to_first_report_ms, "135");
        assert_eq!(stats.avg_submit_to_fill_ms, "650");
        assert_eq!(stats.avg_cancel_ack_latency_ms, "91");
        assert_eq!(stats.decision_latency_samples, 5);
        assert_eq!(stats.submit_ack_latency_samples, 4);
        assert_eq!(stats.submit_to_first_report_samples, 3);
        assert_eq!(stats.submit_to_fill_samples, 2);
        assert_eq!(stats.cancel_ack_latency_samples, 6);
    }

    #[tokio::test]
    async fn recent_execution_reports_expose_separate_latency_fields() {
        let pool = test_pool().await;
        sqlx::query(
            r#"
            INSERT INTO execution_reports (
                event_time, client_order_id, exchange_order_id, symbol, status,
                filled_quantity, average_fill_price, fill_ratio, requested_price,
                slippage_bps, decision_latency_ms, submit_ack_latency_ms,
                submit_to_first_report_ms, submit_to_fill_ms, exchange_order_age_ms, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(OffsetDateTime::UNIX_EPOCH.to_string())
        .bind("bot-latency-1")
        .bind(Some("exchange-1"))
        .bind("BTCUSDC")
        .bind("Filled")
        .bind("0.010")
        .bind(Some("65000"))
        .bind("1")
        .bind(Some("64999"))
        .bind(Some("0.4"))
        .bind(Some(18_i64))
        .bind(Some(72_i64))
        .bind(Some(95_i64))
        .bind(Some(410_i64))
        .bind(Some(5_200_i64))
        .bind(Some("filled"))
        .execute(&pool)
        .await
        .expect("insert execution report");

        let reports = fetch_recent_execution_reports(&pool, None)
            .await
            .expect("recent execution reports");

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].origin, "bot");
        assert_eq!(reports[0].decision_latency_ms, Some(18));
        assert_eq!(reports[0].submit_ack_latency_ms, Some(72));
        assert_eq!(reports[0].submit_to_first_report_ms, Some(95));
        assert_eq!(reports[0].submit_to_fill_ms, Some(410));
        assert_eq!(reports[0].exchange_order_age_ms, Some(5_200));
    }
}
