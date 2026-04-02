use anyhow::Result;
use async_trait::async_trait;
use common::Timestamp;
use domain::FillEvent;
use std::{sync::Arc, vec::Vec};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub label: String,
    pub occurred_at: Timestamp,
}

#[async_trait]
pub trait AccountingService: Send + Sync {
    async fn record_fill(&self, fill: &FillEvent) -> Result<()>;
    async fn journal(&self) -> Vec<LedgerEntry>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryAccountingService {
    entries: Arc<RwLock<Vec<LedgerEntry>>>,
}

#[async_trait]
impl AccountingService for InMemoryAccountingService {
    async fn record_fill(&self, fill: &FillEvent) -> Result<()> {
        self.entries.write().await.push(LedgerEntry {
            label: format!(
                "fill {} {} {} @ {}",
                fill.symbol, fill.trade_id, fill.quantity, fill.price
            ),
            occurred_at: fill.event_time,
        });
        Ok(())
    }

    async fn journal(&self) -> Vec<LedgerEntry> {
        self.entries.read().await.clone()
    }
}
