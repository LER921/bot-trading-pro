use anyhow::Result;
use async_trait::async_trait;
use common::Decimal;
use domain::{
    AccountSnapshot, ExecutionReport, ExecutionStats, FillEvent, InventorySnapshot, PnlSnapshot,
    PositionExitEvent, RiskDecision, RuntimeTransition, StrategyContext, StrategyOutcome,
    SymbolBudget, SystemHealth,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn persist_runtime_transition(&self, transition: &RuntimeTransition) -> Result<()>;
    async fn persist_health_snapshot(&self, health: &SystemHealth) -> Result<()>;
    async fn persist_account_snapshot(&self, snapshot: &AccountSnapshot) -> Result<()>;
    async fn persist_risk_decision(&self, decision: &RiskDecision) -> Result<()>;
    async fn persist_execution_report(&self, report: &ExecutionReport) -> Result<()>;
    async fn persist_execution_stats(&self, stats: &ExecutionStats) -> Result<()>;
    async fn persist_fill(&self, fill: &FillEvent) -> Result<()>;
    async fn persist_inventory_snapshots(
        &self,
        inventory: &[InventorySnapshot],
        budgets: &[SymbolBudget],
    ) -> Result<()>;
    async fn persist_position_exit_event(&self, event: &PositionExitEvent) -> Result<()>;
    async fn persist_strategy_outcome(
        &self,
        strategy: &str,
        context: &StrategyContext,
        outcome: &StrategyOutcome,
    ) -> Result<()>;
    async fn persist_pnl_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}

#[derive(Debug, Default, Clone)]
pub struct MemoryStorage {
    events: Arc<RwLock<Vec<String>>>,
}

#[async_trait]
impl StorageEngine for MemoryStorage {
    async fn persist_runtime_transition(&self, transition: &RuntimeTransition) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("runtime {:?}->{:?}", transition.from, transition.to));
        Ok(())
    }

    async fn persist_health_snapshot(&self, health: &SystemHealth) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("health {:?}", health.overall_state));
        Ok(())
    }

    async fn persist_account_snapshot(&self, snapshot: &AccountSnapshot) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("account {}", snapshot.balances.len()));
        Ok(())
    }

    async fn persist_risk_decision(&self, decision: &RiskDecision) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("risk {:?} {}", decision.action, decision.reason));
        Ok(())
    }

    async fn persist_execution_report(&self, report: &ExecutionReport) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("execution {:?} {}", report.status, report.client_order_id));
        Ok(())
    }

    async fn persist_execution_stats(&self, stats: &ExecutionStats) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("execution-stats {}", stats.total_submitted));
        Ok(())
    }

    async fn persist_fill(&self, fill: &FillEvent) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("fill {} {}", fill.symbol, fill.trade_id));
        Ok(())
    }

    async fn persist_inventory_snapshots(
        &self,
        inventory: &[InventorySnapshot],
        _budgets: &[SymbolBudget],
    ) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("inventory {}", inventory.len()));
        Ok(())
    }

    async fn persist_position_exit_event(&self, event: &PositionExitEvent) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("position-exit {} {}", event.symbol, event.hold_time_ms));
        Ok(())
    }

    async fn persist_strategy_outcome(
        &self,
        strategy: &str,
        context: &StrategyContext,
        outcome: &StrategyOutcome,
    ) -> Result<()> {
        self.events.write().await.push(format!(
            "strategy {} {} {}",
            strategy,
            context.symbol,
            outcome.intents.len()
        ));
        Ok(())
    }

    async fn persist_pnl_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("pnl {} {}", snapshot.net_pnl_quote, snapshot.updated_at));
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SqliteStorage {
    pool: Pool<Sqlite>,
    schema_ready: Arc<OnceCell<()>>,
}

impl SqliteStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let connect_options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);
        let pool = SqlitePoolOptions::new().max_connections(1).connect_lazy_with(connect_options);

        Ok(Self {
            pool,
            schema_ready: Arc::new(OnceCell::new()),
        })
    }

    async fn ensure_schema(&self) -> Result<()> {
        self.schema_ready
            .get_or_try_init(|| async {
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
                .execute(&self.pool)
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
                .execute(&self.pool)
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "health_snapshots",
                    "rest_roundtrip_ms",
                    "INTEGER",
                )
                .await?;

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
                .execute(&self.pool)
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
                .execute(&self.pool)
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
                        exchange_order_age_ms INTEGER,
                        edge_after_cost_bps TEXT,
                        expected_realized_edge_bps TEXT,
                        adverse_selection_penalty_bps TEXT,
                        intent_role TEXT,
                        exit_stage TEXT,
                        exit_reason TEXT
                    );
                    "#,
                )
                .execute(&self.pool)
                .await?;
                add_column_if_missing(&self.pool, "execution_reports", "message", "TEXT").await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "submit_ack_latency_ms",
                    "INTEGER",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "submit_to_first_report_ms",
                    "INTEGER",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "submit_to_fill_ms",
                    "INTEGER",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "exchange_order_age_ms",
                    "INTEGER",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "edge_after_cost_bps",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "expected_realized_edge_bps",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "adverse_selection_penalty_bps",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "intent_role",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "exit_stage",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_reports",
                    "exit_reason",
                    "TEXT",
                )
                .await?;

                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS execution_stats_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        updated_at TEXT NOT NULL,
                        total_submitted INTEGER NOT NULL,
                        total_rejected INTEGER NOT NULL,
                        total_local_validation_rejects INTEGER NOT NULL,
                        total_benign_exchange_rejected INTEGER NOT NULL,
                        risk_scored_rejections INTEGER NOT NULL,
                        total_canceled INTEGER NOT NULL,
                        total_manual_cancels INTEGER NOT NULL,
                        total_external_cancels INTEGER NOT NULL,
                        total_filled_reports INTEGER NOT NULL,
                        total_stale_cancels INTEGER NOT NULL,
                        total_duplicate_intents INTEGER NOT NULL,
                        total_equivalent_order_skips INTEGER NOT NULL,
                        reject_rate TEXT NOT NULL,
                        risk_reject_rate TEXT NOT NULL,
                        avg_fill_ratio TEXT NOT NULL,
                        avg_slippage_bps TEXT NOT NULL,
                        avg_decision_latency_ms TEXT NOT NULL,
                        avg_submit_ack_latency_ms TEXT NOT NULL,
                        avg_submit_to_first_report_ms TEXT NOT NULL,
                        avg_submit_to_fill_ms TEXT NOT NULL,
                        avg_cancel_ack_latency_ms TEXT NOT NULL,
                        decision_latency_samples INTEGER NOT NULL DEFAULT 0,
                        submit_ack_latency_samples INTEGER NOT NULL DEFAULT 0,
                        submit_to_first_report_samples INTEGER NOT NULL DEFAULT 0,
                        submit_to_fill_samples INTEGER NOT NULL DEFAULT 0,
                        cancel_ack_latency_samples INTEGER NOT NULL DEFAULT 0
                    );
                    "#,
                )
                .execute(&self.pool)
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "total_local_validation_rejects",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "total_benign_exchange_rejected",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "risk_scored_rejections",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "total_external_cancels",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "risk_reject_rate",
                    "TEXT NOT NULL DEFAULT '0'",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "avg_submit_ack_latency_ms",
                    "TEXT NOT NULL DEFAULT '0'",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "avg_submit_to_first_report_ms",
                    "TEXT NOT NULL DEFAULT '0'",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "avg_submit_to_fill_ms",
                    "TEXT NOT NULL DEFAULT '0'",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "avg_cancel_ack_latency_ms",
                    "TEXT NOT NULL DEFAULT '0'",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "decision_latency_samples",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "submit_ack_latency_samples",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "submit_to_first_report_samples",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "execution_stats_snapshots",
                    "submit_to_fill_samples",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
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
                .execute(&self.pool)
                .await?;
                add_column_if_missing(&self.pool, "fills", "fee_quote", "TEXT").await?;

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
                        position_opened_at TEXT,
                        last_fill_at TEXT,
                        first_reduce_at TEXT,
                        max_quote_notional TEXT NOT NULL,
                        reserved_quote_notional TEXT NOT NULL,
                        soft_inventory_base TEXT NOT NULL,
                        max_inventory_base TEXT NOT NULL,
                        neutralization_clip_fraction TEXT NOT NULL,
                        reduce_only_trigger_ratio TEXT NOT NULL
                    );
                    "#,
                )
                .execute(&self.pool)
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "inventory_snapshots",
                    "position_opened_at",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "inventory_snapshots",
                    "last_fill_at",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "inventory_snapshots",
                    "first_reduce_at",
                    "TEXT",
                )
                .await?;

                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS position_exit_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        opened_at TEXT NOT NULL,
                        first_exit_at TEXT,
                        closed_at TEXT NOT NULL,
                        hold_time_ms INTEGER NOT NULL,
                        time_to_first_exit_ms INTEGER,
                        intent_role TEXT,
                        exit_stage TEXT,
                        exit_reason TEXT,
                        passive_exit INTEGER NOT NULL,
                        aggressive_exit INTEGER NOT NULL,
                        forced_unwind INTEGER NOT NULL
                    );
                    "#,
                )
                .execute(&self.pool)
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
                        entry_block_reason TEXT,
                        intent_count INTEGER NOT NULL,
                        best_edge_after_cost_bps TEXT NOT NULL,
                        best_expected_realized_edge_bps TEXT,
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
                        max_open_order_slots INTEGER NOT NULL DEFAULT 0,
                        passive_exit_intents INTEGER NOT NULL DEFAULT 0,
                        aggressive_exit_intents INTEGER NOT NULL DEFAULT 0,
                        forced_unwind_intents INTEGER NOT NULL DEFAULT 0,
                        timeout_exit_intents INTEGER NOT NULL DEFAULT 0,
                        reversal_exit_intents INTEGER NOT NULL DEFAULT 0,
                        inventory_pressure_exit_intents INTEGER NOT NULL DEFAULT 0,
                        adverse_exit_intents INTEGER NOT NULL DEFAULT 0,
                        profit_capture_exit_intents INTEGER NOT NULL DEFAULT 0,
                        adverse_selection_hits INTEGER NOT NULL DEFAULT 0,
                        oldest_inventory_age_ms INTEGER,
                        stale_inventory_count INTEGER NOT NULL DEFAULT 0
                    );
                    "#,
                )
                .execute(&self.pool)
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "entry_block_reason",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "best_expected_realized_edge_bps",
                    "TEXT",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "low_edge_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "medium_edge_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "high_edge_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "reduce_risk_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "add_risk_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "open_order_slots_used",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "max_open_order_slots",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "passive_exit_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "aggressive_exit_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "forced_unwind_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "timeout_exit_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "reversal_exit_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "inventory_pressure_exit_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "adverse_exit_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "profit_capture_exit_intents",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "adverse_selection_hits",
                    "INTEGER NOT NULL DEFAULT 0",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "oldest_inventory_age_ms",
                    "INTEGER",
                )
                .await?;
                add_column_if_missing(
                    &self.pool,
                    "strategy_outcomes",
                    "stale_inventory_count",
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
                .execute(&self.pool)
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
                .execute(&self.pool)
                .await?;

                Ok::<(), sqlx::Error>(())
            })
            .await?;
        Ok(())
    }

    pub async fn count_rows(&self, table: &str) -> Result<i64> {
        self.ensure_schema().await?;
        let row = sqlx::query(&format!("SELECT COUNT(*) AS count FROM {table}"))
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("count"))
    }
}

#[async_trait]
impl StorageEngine for SqliteStorage {
    async fn persist_runtime_transition(&self, transition: &RuntimeTransition) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            "INSERT INTO runtime_transitions (transitioned_at, from_state, to_state, reason) VALUES (?, ?, ?, ?)",
        )
        .bind(transition.transitioned_at.to_string())
        .bind(format!("{:?}", transition.from))
        .bind(format!("{:?}", transition.to))
        .bind(&transition.reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_health_snapshot(&self, health: &SystemHealth) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            r#"
            INSERT INTO health_snapshots (
                observed_at, overall_state, market_ws_state, market_ws_age_ms,
                market_ws_reconnect_count, user_ws_state, user_ws_age_ms,
                user_ws_reconnect_count, rest_state, rest_consecutive_failures,
                market_data_state, account_events_state, clock_drift_state,
                clock_drift_ms, fallback_active, rest_roundtrip_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(health.updated_at.to_string())
        .bind(format!("{:?}", health.overall_state))
        .bind(format!("{:?}", health.market_ws.state))
        .bind(health.market_ws.last_event_age_ms)
        .bind(i64::try_from(health.market_ws.reconnect_count).unwrap_or(i64::MAX))
        .bind(format!("{:?}", health.user_ws.state))
        .bind(health.user_ws.last_event_age_ms)
        .bind(i64::try_from(health.user_ws.reconnect_count).unwrap_or(i64::MAX))
        .bind(format!("{:?}", health.rest.state))
        .bind(i64::try_from(health.rest.consecutive_failures).unwrap_or(i64::MAX))
        .bind(format!("{:?}", health.market_data.state))
        .bind(format!("{:?}", health.account_events.state))
        .bind(format!("{:?}", health.clock_drift.state))
        .bind(health.clock_drift.drift_ms)
        .bind(if health.fallback_active { 1_i64 } else { 0_i64 })
        .bind(health.rest.roundtrip_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_account_snapshot(&self, snapshot: &AccountSnapshot) -> Result<()> {
        self.ensure_schema().await?;
        for balance in &snapshot.balances {
            sqlx::query(
                "INSERT INTO account_snapshots (updated_at, asset, free, locked) VALUES (?, ?, ?, ?)",
            )
            .bind(snapshot.updated_at.to_string())
            .bind(&balance.asset)
            .bind(balance.free.to_string())
            .bind(balance.locked.to_string())
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn persist_risk_decision(&self, decision: &RiskDecision) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            "INSERT INTO risk_decisions (decided_at, action, symbol, resulting_mode, reason) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(decision.decided_at.to_string())
        .bind(format!("{:?}", decision.action))
        .bind(decision.original_intent.symbol.as_str())
        .bind(format!("{:?}", decision.resulting_mode))
        .bind(&decision.reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_execution_report(&self, report: &ExecutionReport) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            r#"
            INSERT INTO execution_reports (
                event_time, client_order_id, exchange_order_id, symbol, status,
                filled_quantity, average_fill_price, fill_ratio, requested_price,
                slippage_bps, decision_latency_ms, submit_ack_latency_ms,
                submit_to_first_report_ms, submit_to_fill_ms, exchange_order_age_ms,
                edge_after_cost_bps, expected_realized_edge_bps, adverse_selection_penalty_bps,
                intent_role, exit_stage, exit_reason, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(report.event_time.to_string())
        .bind(&report.client_order_id)
        .bind(&report.exchange_order_id)
        .bind(report.symbol.as_str())
        .bind(format!("{:?}", report.status))
        .bind(report.filled_quantity.to_string())
        .bind(report.average_fill_price.map(|value| value.to_string()))
        .bind(report.fill_ratio.to_string())
        .bind(report.requested_price.map(|value| value.to_string()))
        .bind(report.slippage_bps.map(|value| value.to_string()))
        .bind(report.decision_latency_ms)
        .bind(report.submit_ack_latency_ms)
        .bind(report.submit_to_first_report_ms)
        .bind(report.submit_to_fill_ms)
        .bind(report.exchange_order_age_ms)
        .bind(report.edge_after_cost_bps.map(|value| value.to_string()))
        .bind(report.expected_realized_edge_bps.map(|value| value.to_string()))
        .bind(report.adverse_selection_penalty_bps.map(|value| value.to_string()))
        .bind(report.intent_role.map(|value| format!("{value:?}")))
        .bind(report.exit_stage.map(|value| format!("{value:?}")))
        .bind(&report.exit_reason)
        .bind(&report.message)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_execution_stats(&self, stats: &ExecutionStats) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            r#"
            INSERT INTO execution_stats_snapshots (
                updated_at, total_submitted, total_rejected, total_local_validation_rejects,
                total_benign_exchange_rejected, risk_scored_rejections, total_canceled,
                total_manual_cancels, total_external_cancels, total_filled_reports,
                total_stale_cancels, total_duplicate_intents, total_equivalent_order_skips,
                reject_rate, risk_reject_rate, avg_fill_ratio, avg_slippage_bps,
                avg_decision_latency_ms, avg_submit_ack_latency_ms,
                avg_submit_to_first_report_ms, avg_submit_to_fill_ms, avg_cancel_ack_latency_ms,
                decision_latency_samples, submit_ack_latency_samples,
                submit_to_first_report_samples, submit_to_fill_samples,
                cancel_ack_latency_samples
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(stats.updated_at.to_string())
        .bind(i64::try_from(stats.total_submitted).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_rejected).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_local_validation_rejects).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_benign_exchange_rejected).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.risk_scored_rejections).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_canceled).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_manual_cancels).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_external_cancels).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_filled_reports).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_stale_cancels).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_duplicate_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.total_equivalent_order_skips).unwrap_or(i64::MAX))
        .bind(stats.reject_rate.to_string())
        .bind(stats.risk_reject_rate.to_string())
        .bind(stats.avg_fill_ratio.to_string())
        .bind(stats.avg_slippage_bps.to_string())
        .bind(stats.avg_decision_latency_ms.to_string())
        .bind(stats.avg_submit_ack_latency_ms.to_string())
        .bind(stats.avg_submit_to_first_report_ms.to_string())
        .bind(stats.avg_submit_to_fill_ms.to_string())
        .bind(stats.avg_cancel_ack_latency_ms.to_string())
        .bind(i64::try_from(stats.decision_latency_samples).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.submit_ack_latency_samples).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.submit_to_first_report_samples).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.submit_to_fill_samples).unwrap_or(i64::MAX))
        .bind(i64::try_from(stats.cancel_ack_latency_samples).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_fill(&self, fill: &FillEvent) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            "INSERT INTO fills (event_time, trade_id, order_id, symbol, side, price, quantity, fee, fee_asset, fee_quote) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(fill.event_time.to_string())
        .bind(&fill.trade_id)
        .bind(&fill.order_id)
        .bind(fill.symbol.as_str())
        .bind(format!("{:?}", fill.side))
        .bind(fill.price.to_string())
        .bind(fill.quantity.to_string())
        .bind(fill.fee.to_string())
        .bind(&fill.fee_asset)
        .bind(fill.fee_quote.map(|value| value.to_string()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_inventory_snapshots(
        &self,
        inventory: &[InventorySnapshot],
        budgets: &[SymbolBudget],
    ) -> Result<()> {
        self.ensure_schema().await?;
        let budgets_by_symbol = budgets
            .iter()
            .map(|budget| (budget.symbol, budget))
            .collect::<HashMap<_, _>>();

        for snapshot in inventory {
            let Some(budget) = budgets_by_symbol.get(&snapshot.symbol) else {
                continue;
            };
            sqlx::query(
                r#"
                INSERT INTO inventory_snapshots (
                    observed_at, symbol, base_position, quote_position, mark_price,
                    average_entry_price, position_opened_at, last_fill_at, first_reduce_at,
                    max_quote_notional, reserved_quote_notional,
                    soft_inventory_base, max_inventory_base,
                    neutralization_clip_fraction, reduce_only_trigger_ratio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(snapshot.updated_at.to_string())
            .bind(snapshot.symbol.as_str())
            .bind(snapshot.base_position.to_string())
            .bind(snapshot.quote_position.to_string())
            .bind(snapshot.mark_price.map(|value| value.to_string()))
            .bind(snapshot.average_entry_price.map(|value| value.to_string()))
            .bind(snapshot.position_opened_at.map(|value| value.to_string()))
            .bind(snapshot.last_fill_at.map(|value| value.to_string()))
            .bind(snapshot.first_reduce_at.map(|value| value.to_string()))
            .bind(budget.max_quote_notional.to_string())
            .bind(budget.reserved_quote_notional.to_string())
            .bind(budget.soft_inventory_base.to_string())
            .bind(budget.max_inventory_base.to_string())
            .bind(budget.neutralization_clip_fraction.to_string())
            .bind(budget.reduce_only_trigger_ratio.to_string())
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn persist_position_exit_event(&self, event: &PositionExitEvent) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            r#"
            INSERT INTO position_exit_events (
                symbol, opened_at, first_exit_at, closed_at, hold_time_ms,
                time_to_first_exit_ms, intent_role, exit_stage, exit_reason,
                passive_exit, aggressive_exit, forced_unwind
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(event.symbol.as_str())
        .bind(event.opened_at.to_string())
        .bind(event.first_exit_at.map(|value| value.to_string()))
        .bind(event.closed_at.to_string())
        .bind(event.hold_time_ms)
        .bind(event.time_to_first_exit_ms)
        .bind(event.intent_role.map(|value| format!("{value:?}")))
        .bind(event.exit_stage.map(|value| format!("{value:?}")))
        .bind(&event.exit_reason)
        .bind(if event.passive_exit { 1_i64 } else { 0_i64 })
        .bind(if event.aggressive_exit { 1_i64 } else { 0_i64 })
        .bind(if event.forced_unwind { 1_i64 } else { 0_i64 })
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_strategy_outcome(
        &self,
        strategy: &str,
        context: &StrategyContext,
        outcome: &StrategyOutcome,
    ) -> Result<()> {
        self.ensure_schema().await?;
        let best_edge_after_cost_bps = outcome
            .intents
            .iter()
            .map(|intent| intent.edge_after_cost_bps)
            .max()
            .unwrap_or_default();
        let best_expected_realized_edge_bps =
            outcome.best_expected_realized_edge_bps.unwrap_or_default();
        let reduce_only_intents = outcome
            .intents
            .iter()
            .filter(|intent| intent.reduce_only)
            .count();
        let low_edge_intents = outcome
            .intents
            .iter()
            .filter(|intent| intent.edge_after_cost_bps < dec("2.0"))
            .count();
        let medium_edge_intents = outcome
            .intents
            .iter()
            .filter(|intent| intent.edge_after_cost_bps >= dec("2.0") && intent.edge_after_cost_bps < dec("5.0"))
            .count();
        let high_edge_intents = outcome
            .intents
            .iter()
            .filter(|intent| intent.edge_after_cost_bps >= dec("5.0"))
            .count();
        let reduce_risk_intents = outcome
            .intents
            .iter()
            .filter(|intent| intent_reduces_inventory(context.inventory.base_position, intent))
            .count();
        let add_risk_intents = outcome.intents.len().saturating_sub(reduce_risk_intents);
        let passive_exit_intents = outcome
            .intents
            .iter()
            .filter(|intent| matches!(intent.exit_stage, Some(domain::ExitStage::Passive)))
            .count();
        let aggressive_exit_intents = outcome
            .intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.exit_stage,
                    Some(domain::ExitStage::Aggressive | domain::ExitStage::Emergency)
                )
            })
            .count();
        let forced_unwind_intents = outcome
            .intents
            .iter()
            .filter(|intent| {
                matches!(
                    intent.role,
                    domain::IntentRole::ForcedUnwind | domain::IntentRole::EmergencyExit
                )
            })
            .count();
        let timeout_exit_intents = outcome
            .intents
            .iter()
            .filter(|intent| {
                intent
                    .exit_reason
                    .as_deref()
                    .unwrap_or_default()
                    .contains("hold")
                    || intent.reason.contains("hold")
                    || intent.reason.contains("stale")
            })
            .count();
        let reversal_exit_intents = outcome
            .intents
            .iter()
            .filter(|intent| {
                intent.reason.contains("reversal") || intent.reason.contains("invalidation")
            })
            .count();
        let inventory_pressure_exit_intents = outcome
            .intents
            .iter()
            .filter(|intent| {
                intent.reason.contains("inventory pressure") || intent.reason.contains("inventory")
            })
            .count();
        let adverse_exit_intents = outcome
            .intents
            .iter()
            .filter(|intent| {
                intent.reason.contains("toxicity")
                    || intent.reason.contains("reversal")
                    || intent.reason.contains("adverse")
            })
            .count();
        let profit_capture_exit_intents = outcome
            .intents
            .iter()
            .filter(|intent| {
                matches!(intent.role, domain::IntentRole::PassiveProfitTake)
                    || intent.reason.contains("take-profit")
                    || intent.reason.contains("profit-take")
            })
            .count();
        let oldest_inventory_age_ms = context.inventory.position_opened_at.map(|opened_at| {
            (context.features.computed_at - opened_at).whole_milliseconds() as i64
        });
        let stale_inventory_count = oldest_inventory_age_ms
            .map(|age_ms| usize::from(age_ms >= 30_000))
            .unwrap_or(0);
        let status = if outcome.intents.is_empty() {
            "standby"
        } else {
            "actionable"
        };
        let standby_reason = outcome
            .standby_reason
            .clone()
            .unwrap_or_else(|| format!("{strategy} produced actionable intents"));
        let entry_block_reason = outcome.entry_block_reason.clone();
        let sample_reason = outcome
            .intents
            .first()
            .map(|intent| intent.reason.clone())
            .unwrap_or_else(|| standby_reason.clone());

        sqlx::query(
            r#"
            INSERT INTO strategy_outcomes (
                observed_at, symbol, strategy, status, standby_reason, entry_block_reason, intent_count,
                best_edge_after_cost_bps, best_expected_realized_edge_bps, reduce_only_intents, sample_reason,
                runtime_state, risk_mode, inventory_base, spread_bps, toxicity_score,
                local_momentum_bps, trade_flow_imbalance, orderbook_imbalance,
                low_edge_intents, medium_edge_intents, high_edge_intents,
                reduce_risk_intents, add_risk_intents, open_order_slots_used, max_open_order_slots,
                passive_exit_intents, aggressive_exit_intents, forced_unwind_intents,
                timeout_exit_intents, reversal_exit_intents, inventory_pressure_exit_intents,
                adverse_exit_intents, profit_capture_exit_intents, adverse_selection_hits, oldest_inventory_age_ms,
                stale_inventory_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(context.features.computed_at.to_string())
        .bind(context.symbol.as_str())
        .bind(strategy)
        .bind(status)
        .bind(standby_reason)
        .bind(entry_block_reason)
        .bind(i64::try_from(outcome.intents.len()).unwrap_or(i64::MAX))
        .bind(best_edge_after_cost_bps.to_string())
        .bind(best_expected_realized_edge_bps.to_string())
        .bind(i64::try_from(reduce_only_intents).unwrap_or(i64::MAX))
        .bind(sample_reason)
        .bind(format!("{:?}", context.runtime_state))
        .bind(format!("{:?}", context.risk_mode))
        .bind(context.inventory.base_position.to_string())
        .bind(context.features.spread_bps.to_string())
        .bind(context.features.toxicity_score.to_string())
        .bind(context.features.local_momentum_bps.to_string())
        .bind(context.features.trade_flow_imbalance.to_string())
        .bind(context.features.orderbook_imbalance.map(|value| value.to_string()))
        .bind(i64::try_from(low_edge_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(medium_edge_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(high_edge_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(reduce_risk_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(add_risk_intents).unwrap_or(i64::MAX))
        .bind(i64::from(context.open_bot_orders_for_symbol))
        .bind(i64::from(context.max_open_orders_for_symbol))
        .bind(i64::try_from(passive_exit_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(aggressive_exit_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(forced_unwind_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(timeout_exit_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(reversal_exit_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(inventory_pressure_exit_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(adverse_exit_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(profit_capture_exit_intents).unwrap_or(i64::MAX))
        .bind(i64::try_from(outcome.adverse_selection_hits).unwrap_or(i64::MAX))
        .bind(oldest_inventory_age_ms)
        .bind(i64::try_from(stale_inventory_count).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_pnl_snapshot(&self, snapshot: &PnlSnapshot) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query(
            "INSERT INTO pnl_snapshots (updated_at, realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote, fees_quote, daily_pnl_quote, peak_net_pnl_quote, drawdown_quote) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(snapshot.updated_at.to_string())
        .bind(snapshot.realized_pnl_quote.to_string())
        .bind(snapshot.unrealized_pnl_quote.to_string())
        .bind(snapshot.net_pnl_quote.to_string())
        .bind(snapshot.fees_quote.to_string())
        .bind(snapshot.daily_pnl_quote.to_string())
        .bind(snapshot.peak_net_pnl_quote.to_string())
        .bind(snapshot.drawdown_quote.to_string())
        .execute(&self.pool)
        .await?;

        for symbol_snapshot in &snapshot.per_symbol {
            sqlx::query(
                r#"
                INSERT INTO pnl_symbol_snapshots (
                    updated_at, symbol, position_base, average_entry_price, mark_price,
                    realized_pnl_quote, unrealized_pnl_quote, net_pnl_quote, fees_quote,
                    peak_net_pnl_quote, drawdown_quote
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(snapshot.updated_at.to_string())
            .bind(symbol_snapshot.symbol.as_str())
            .bind(symbol_snapshot.position_base.to_string())
            .bind(symbol_snapshot.average_entry_price.map(|value| value.to_string()))
            .bind(symbol_snapshot.mark_price.map(|value| value.to_string()))
            .bind(symbol_snapshot.realized_pnl_quote.to_string())
            .bind(symbol_snapshot.unrealized_pnl_quote.to_string())
            .bind(symbol_snapshot.net_pnl_quote.to_string())
            .bind(symbol_snapshot.fees_quote.to_string())
            .bind(symbol_snapshot.peak_net_pnl_quote.to_string())
            .bind(symbol_snapshot.drawdown_quote.to_string())
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.ensure_schema().await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE);")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

async fn add_column_if_missing(
    pool: &Pool<Sqlite>,
    table: &str,
    column: &str,
    column_type: &str,
) -> Result<(), sqlx::Error> {
    let statement = format!("ALTER TABLE {table} ADD COLUMN {column} {column_type};");
    if let Err(error) = sqlx::query(&statement).execute(pool).await {
        if !is_duplicate_column_error(&error, column) {
            return Err(error);
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

fn intent_reduces_inventory(position: Decimal, intent: &domain::TradeIntent) -> bool {
    intent.reduce_only
        || (position > Decimal::ZERO && matches!(intent.side, domain::Side::Sell))
        || (position < Decimal::ZERO && matches!(intent.side, domain::Side::Buy))
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Decimal, now_utc};
    use domain::{
        AccountSnapshot, Balance, ExecutionReport, ExitStage, IntentRole, OrderStatus,
        PositionExitEvent, RuntimeState, RuntimeTransition, Symbol, SymbolPnlSnapshot,
    };
    use tokio::fs;

    #[tokio::test]
    async fn sqlite_storage_persists_runtime_and_execution_rows() {
        let temp_dir = std::env::temp_dir().join("bot_trading_pro_storage_test");
        let _ = fs::remove_dir_all(&temp_dir).await;
        let storage = SqliteStorage::new(temp_dir.join("bot.db")).unwrap();

        storage
            .persist_runtime_transition(&RuntimeTransition {
                from: RuntimeState::Bootstrap,
                to: RuntimeState::Ready,
                reason: "test".to_string(),
                transitioned_at: now_utc(),
            })
            .await
            .unwrap();
        storage
            .persist_execution_report(&ExecutionReport {
                client_order_id: "bot-1".to_string(),
                exchange_order_id: Some("1".to_string()),
                symbol: Symbol::BtcUsdc,
                status: OrderStatus::New,
                filled_quantity: Decimal::ZERO,
                average_fill_price: None,
                fill_ratio: Decimal::ZERO,
                requested_price: Some(Decimal::from(60_000u32)),
                slippage_bps: None,
                decision_latency_ms: Some(10),
                submit_ack_latency_ms: Some(15),
                submit_to_first_report_ms: None,
                submit_to_fill_ms: None,
                exchange_order_age_ms: None,
                edge_after_cost_bps: Some(Decimal::from_str_exact("1.10").unwrap()),
                expected_realized_edge_bps: Some(Decimal::from_str_exact("0.72").unwrap()),
                adverse_selection_penalty_bps: Some(Decimal::from_str_exact("0.18").unwrap()),
                intent_role: Some(IntentRole::AddRisk),
                exit_stage: None,
                exit_reason: None,
                message: Some("accepted".to_string()),
                event_time: now_utc(),
            })
            .await
            .unwrap();

        assert_eq!(storage.count_rows("runtime_transitions").await.unwrap(), 1);
        assert_eq!(storage.count_rows("execution_reports").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn sqlite_storage_persists_symbol_pnl_rows() {
        let temp_dir = std::env::temp_dir().join("bot_trading_pro_storage_test_symbol_pnl");
        let _ = fs::remove_dir_all(&temp_dir).await;
        let storage = SqliteStorage::new(temp_dir.join("bot.db")).unwrap();

        storage
            .persist_pnl_snapshot(&PnlSnapshot {
                realized_pnl_quote: Decimal::from_str_exact("5").unwrap(),
                unrealized_pnl_quote: Decimal::from_str_exact("2").unwrap(),
                net_pnl_quote: Decimal::from_str_exact("6").unwrap(),
                fees_quote: Decimal::from_str_exact("1").unwrap(),
                daily_pnl_quote: Decimal::from_str_exact("6").unwrap(),
                peak_net_pnl_quote: Decimal::from_str_exact("6").unwrap(),
                drawdown_quote: Decimal::ZERO,
                per_symbol: vec![SymbolPnlSnapshot {
                    symbol: Symbol::BtcUsdc,
                    position_base: Decimal::from_str_exact("0.01").unwrap(),
                    average_entry_price: Some(Decimal::from(60_000u32)),
                    mark_price: Some(Decimal::from(60_100u32)),
                    realized_pnl_quote: Decimal::from_str_exact("5").unwrap(),
                    unrealized_pnl_quote: Decimal::from_str_exact("2").unwrap(),
                    net_pnl_quote: Decimal::from_str_exact("6").unwrap(),
                    fees_quote: Decimal::from_str_exact("1").unwrap(),
                    peak_net_pnl_quote: Decimal::from_str_exact("6").unwrap(),
                    drawdown_quote: Decimal::ZERO,
                }],
                updated_at: now_utc(),
            })
            .await
            .unwrap();

        assert_eq!(storage.count_rows("pnl_snapshots").await.unwrap(), 1);
        assert_eq!(storage.count_rows("pnl_symbol_snapshots").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn sqlite_storage_persists_account_snapshot_rows() {
        let temp_dir = std::env::temp_dir().join("bot_trading_pro_storage_test_account_snapshot");
        let _ = fs::remove_dir_all(&temp_dir).await;
        let storage = SqliteStorage::new(temp_dir.join("bot.db")).unwrap();

        storage
            .persist_account_snapshot(&AccountSnapshot {
                balances: vec![
                    Balance {
                        asset: "USDC".to_string(),
                        free: Decimal::from(1000u32),
                        locked: Decimal::from(10u32),
                    },
                    Balance {
                        asset: "BTC".to_string(),
                        free: Decimal::from_str_exact("0.001").unwrap(),
                        locked: Decimal::ZERO,
                    },
                ],
                updated_at: now_utc(),
            })
            .await
            .unwrap();

        assert_eq!(storage.count_rows("account_snapshots").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn sqlite_storage_persists_position_exit_events() {
        let temp_dir = std::env::temp_dir().join("bot_trading_pro_storage_test_position_exit");
        let _ = fs::remove_dir_all(&temp_dir).await;
        let storage = SqliteStorage::new(temp_dir.join("bot.db")).unwrap();
        let opened_at = now_utc();
        let first_exit_at = opened_at;
        let closed_at = opened_at;

        storage
            .persist_position_exit_event(&PositionExitEvent {
                symbol: Symbol::BtcUsdc,
                opened_at,
                first_exit_at: Some(first_exit_at),
                closed_at,
                hold_time_ms: 31_000,
                time_to_first_exit_ms: Some(12_000),
                intent_role: Some(IntentRole::ForcedUnwind),
                exit_stage: Some(ExitStage::Aggressive),
                exit_reason: Some("market making aggressive unwind".to_string()),
                passive_exit: false,
                aggressive_exit: true,
                forced_unwind: true,
            })
            .await
            .unwrap();

        assert_eq!(storage.count_rows("position_exit_events").await.unwrap(), 1);
    }
}
