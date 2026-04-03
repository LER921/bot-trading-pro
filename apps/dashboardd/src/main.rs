use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
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
    total_canceled: u64,
    total_manual_cancels: u64,
    total_filled_reports: u64,
    total_stale_cancels: u64,
    total_duplicate_intents: u64,
    total_equivalent_order_skips: u64,
    reject_rate: String,
    avg_fill_ratio: String,
    avg_slippage_bps: String,
    avg_decision_latency_ms: String,
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
    status: String,
    filled_quantity: String,
    average_fill_price: Option<String>,
    fill_ratio: String,
    requested_price: Option<String>,
    slippage_bps: Option<String>,
    decision_latency_ms: Option<i64>,
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
    total_fills: u64,
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
    let pnl_latest = fetch_latest_pnl_snapshot(&state.pool).await?;
    let pnl_history = fetch_pnl_history(&state.pool).await?;
    let pnl_by_symbol = fetch_symbol_pnl(&state.pool).await?;
    let win_rate = fetch_win_rate(&state.pool).await?;
    let inventory = fetch_inventory(&state.pool).await?;
    let latest_stats = fetch_execution_stats(&state.pool).await?;
    let recent_reports = fetch_recent_execution_reports(&state.pool).await?;
    let open_orders = fetch_open_orders(&state.pool).await?;
    let recent_fills = fetch_recent_fills(&state.pool).await?;
    let status_counts = fetch_execution_status_counts(&state.pool).await?;
    let total_fills = fetch_total_fills(&state.pool).await?;
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
            total_fills,
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
        sqlx::query("SELECT transitioned_at FROM runtime_transitions ORDER BY id ASC LIMIT 1")
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
        started_at: started_at.map(|row| row.get::<String, _>("transitioned_at")),
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
        market_data_state: row.get("market_data_state"),
        account_events_state: row.get("account_events_state"),
        clock_drift_state: row.get("clock_drift_state"),
        clock_drift_ms: row.get("clock_drift_ms"),
        fallback_active: row.get::<i64, _>("fallback_active") != 0,
    }))
}

async fn fetch_latest_pnl_snapshot(pool: &Pool<Sqlite>) -> Result<Option<PnlSnapshotView>> {
    let row = default_if_missing(
        sqlx::query(
            r#"
            SELECT updated_at, realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote,
                   fees_quote, daily_pnl_quote, peak_net_pnl_quote, drawdown_quote
            FROM pnl_snapshots
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(pool)
        .await,
    )?;

    Ok(row.map(|row| PnlSnapshotView {
        updated_at: row.get("updated_at"),
        realized_pnl_quote: row.get("realized_pnl_quote"),
        unrealized_pnl_quote: row.get("unrealized_pnl_quote"),
        net_pnl_quote: row.get("net_pnl_quote"),
        fees_quote: row.get("fees_quote"),
        daily_pnl_quote: row.get("daily_pnl_quote"),
        peak_net_pnl_quote: row.get("peak_net_pnl_quote"),
        drawdown_quote: row.get("drawdown_quote"),
    }))
}

async fn fetch_pnl_history(pool: &Pool<Sqlite>) -> Result<Vec<PnlHistoryPoint>> {
    let mut rows = default_if_missing(
        sqlx::query(
            "SELECT updated_at, net_pnl_quote FROM pnl_snapshots ORDER BY id DESC LIMIT 80",
        )
        .fetch_all(pool)
        .await,
    )?;
    rows.reverse();
    Ok(rows
        .into_iter()
        .map(|row| PnlHistoryPoint {
            updated_at: row.get("updated_at"),
            net_pnl_quote: parse_f64(&row.get::<String, _>("net_pnl_quote")).unwrap_or_default(),
        })
        .collect())
}

async fn fetch_symbol_pnl(pool: &Pool<Sqlite>) -> Result<Vec<SymbolPnlView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            WITH latest AS (
                SELECT symbol, MAX(id) AS max_id
                FROM pnl_symbol_snapshots
                GROUP BY symbol
            )
            SELECT p.updated_at, p.symbol, p.position_base, p.average_entry_price, p.mark_price,
                   p.realized_pnl_quote, p.unrealized_pnl_quote, p.net_pnl_quote, p.fees_quote,
                   p.peak_net_pnl_quote, p.drawdown_quote
            FROM pnl_symbol_snapshots p
            JOIN latest l ON l.max_id = p.id
            ORDER BY p.symbol ASC
            "#,
        )
        .fetch_all(pool)
        .await,
    )?;

    Ok(rows
        .into_iter()
        .map(|row| SymbolPnlView {
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
        })
        .collect())
}

async fn fetch_win_rate(pool: &Pool<Sqlite>) -> Result<WinRateView> {
    let mut rows = default_if_missing(
        sqlx::query(
            "SELECT symbol, updated_at, realized_pnl_quote FROM pnl_symbol_snapshots ORDER BY id DESC LIMIT 1000",
        )
        .fetch_all(pool)
        .await,
    )?;
    rows.reverse();

    let mut previous_realized = HashMap::<String, f64>::new();
    let mut wins = 0u64;
    let mut losses = 0u64;

    for row in rows {
        let symbol = row.get::<String, _>("symbol");
        let realized = parse_f64(&row.get::<String, _>("realized_pnl_quote")).unwrap_or_default();
        if let Some(previous) = previous_realized.insert(symbol, realized) {
            let delta = realized - previous;
            if delta > 0.0000001 {
                wins += 1;
            } else if delta < -0.0000001 {
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
            SELECT updated_at, total_submitted, total_rejected, total_canceled,
                   total_manual_cancels, total_filled_reports, total_stale_cancels,
                   total_duplicate_intents, total_equivalent_order_skips, reject_rate,
                   avg_fill_ratio, avg_slippage_bps, avg_decision_latency_ms
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
            total_canceled,
            total_manual_cancels: row.get::<i64, _>("total_manual_cancels").max(0) as u64,
            total_filled_reports,
            total_stale_cancels,
            total_duplicate_intents,
            total_equivalent_order_skips,
            reject_rate: row.get("reject_rate"),
            avg_fill_ratio: row.get("avg_fill_ratio"),
            avg_slippage_bps: row.get("avg_slippage_bps"),
            avg_decision_latency_ms: row.get("avg_decision_latency_ms"),
            fill_report_rate,
            cancel_rate,
            stale_cancel_share,
            duplicate_suppressions: total_duplicate_intents + total_equivalent_order_skips,
            churn_state: churn_state(fill_report_rate, cancel_rate),
        }
    }))
}

async fn fetch_recent_execution_reports(pool: &Pool<Sqlite>) -> Result<Vec<ExecutionReportView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            SELECT event_time, client_order_id, exchange_order_id, symbol, status,
                   filled_quantity, average_fill_price, fill_ratio, requested_price,
                   slippage_bps, decision_latency_ms, message
            FROM execution_reports
            ORDER BY id DESC
            LIMIT 25
            "#,
        )
        .fetch_all(pool)
        .await,
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
                   e.slippage_bps, e.decision_latency_ms, e.message
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

async fn fetch_recent_fills(pool: &Pool<Sqlite>) -> Result<Vec<FillView>> {
    let rows = default_if_missing(
        sqlx::query(
            r#"
            SELECT event_time, trade_id, order_id, symbol, side, price, quantity, fee, fee_asset, fee_quote
            FROM fills
            ORDER BY id DESC
            LIMIT 40
            "#,
        )
        .fetch_all(pool)
        .await,
    )?;
    Ok(rows
        .into_iter()
        .map(|row| FillView {
            event_time: row.get("event_time"),
            trade_id: row.get("trade_id"),
            order_id: row.get("order_id"),
            symbol: row.get("symbol"),
            side: row.get("side"),
            price: row.get("price"),
            quantity: row.get("quantity"),
            fee: row.get("fee"),
            fee_asset: row.get("fee_asset"),
            fee_quote: row.get("fee_quote"),
        })
        .collect())
}

async fn fetch_total_fills(pool: &Pool<Sqlite>) -> Result<u64> {
    let row = default_if_missing(
        sqlx::query("SELECT COUNT(*) AS count FROM fills")
            .fetch_optional(pool)
            .await,
    )?;
    Ok(row
        .map(|row| row.get::<i64, _>("count").max(0) as u64)
        .unwrap_or_default())
}

async fn fetch_execution_status_counts(pool: &Pool<Sqlite>) -> Result<Vec<StatusCountView>> {
    let rows = default_if_missing(
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
        .await,
    )?;
    Ok(rows
        .into_iter()
        .map(|row| StatusCountView {
            status: row.get("status"),
            count: row.get::<i64, _>("count").max(0) as u64,
        })
        .collect())
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
                   s.toxicity_score, s.local_momentum_bps, s.trade_flow_imbalance, s.orderbook_imbalance
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
                   toxicity_score, local_momentum_bps, trade_flow_imbalance, orderbook_imbalance
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
    ExecutionReportView {
        event_time: row.get("event_time"),
        client_order_id: row.get("client_order_id"),
        exchange_order_id: row.get("exchange_order_id"),
        symbol: row.get("symbol"),
        status: row.get("status"),
        filled_quantity: row.get("filled_quantity"),
        average_fill_price: row.get("average_fill_price"),
        fill_ratio: row.get("fill_ratio"),
        requested_price: row.get("requested_price"),
        slippage_bps: row.get("slippage_bps"),
        decision_latency_ms: row.get("decision_latency_ms"),
        message: row.get("message"),
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

fn parse_f64(raw: &str) -> Option<f64> {
    raw.parse::<f64>().ok()
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
            fallback_active INTEGER NOT NULL
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
            decision_latency_ms INTEGER
        );
        "#,
    )
    .execute(pool)
    .await?;
    add_column_if_missing(pool, "execution_reports", "message", "TEXT").await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS execution_stats_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at TEXT NOT NULL,
            total_submitted INTEGER NOT NULL,
            total_rejected INTEGER NOT NULL,
            total_canceled INTEGER NOT NULL,
            total_manual_cancels INTEGER NOT NULL,
            total_filled_reports INTEGER NOT NULL,
            total_stale_cancels INTEGER NOT NULL,
            total_duplicate_intents INTEGER NOT NULL,
            total_equivalent_order_skips INTEGER NOT NULL,
            reject_rate TEXT NOT NULL,
            avg_fill_ratio TEXT NOT NULL,
            avg_slippage_bps TEXT NOT NULL,
            avg_decision_latency_ms TEXT NOT NULL
        );
        "#,
    )
    .execute(pool)
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
            orderbook_imbalance TEXT
        );
        "#,
    )
    .execute(pool)
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
                local_momentum_bps, trade_flow_imbalance, orderbook_imbalance
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
}
