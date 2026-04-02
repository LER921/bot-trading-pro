use anyhow::Result;
use async_trait::async_trait;
use domain::{ExecutionReport, FillEvent, PnlSnapshot, RiskDecision, RuntimeTransition};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn persist_runtime_transition(&self, transition: &RuntimeTransition) -> Result<()>;
    async fn persist_risk_decision(&self, decision: &RiskDecision) -> Result<()>;
    async fn persist_execution_report(&self, report: &ExecutionReport) -> Result<()>;
    async fn persist_fill(&self, fill: &FillEvent) -> Result<()>;
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

    async fn persist_fill(&self, fill: &FillEvent) -> Result<()> {
        self.events
            .write()
            .await
            .push(format!("fill {} {}", fill.symbol, fill.trade_id));
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
                        decision_latency_ms INTEGER
                    );
                    "#,
                )
                .execute(&self.pool)
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
                        fee_asset TEXT NOT NULL,
                        fee_quote TEXT
                    );
                    "#,
                )
                .execute(&self.pool)
                .await?;

                if let Err(error) = sqlx::query("ALTER TABLE fills ADD COLUMN fee_quote TEXT;")
                    .execute(&self.pool)
                    .await
                {
                    let is_duplicate_column =
                        error.to_string().to_ascii_lowercase().contains("duplicate column name: fee_quote");
                    if !is_duplicate_column {
                        return Err(error);
                    }
                }

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
                slippage_bps, decision_latency_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Decimal, now_utc};
    use domain::{ExecutionReport, OrderStatus, RuntimeState, RuntimeTransition, Symbol};
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
                message: None,
                event_time: now_utc(),
            })
            .await
            .unwrap();

        assert_eq!(storage.count_rows("runtime_transitions").await.unwrap(), 1);
        assert_eq!(storage.count_rows("execution_reports").await.unwrap(), 1);
    }
}
