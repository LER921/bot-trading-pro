use serde::{Deserialize, Serialize};
use std::{future::Future, time::Duration};
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 2_000,
        }
    }
}

pub async fn retry_async<F, Fut, T, E>(policy: &RetryPolicy, mut operation: F) -> Result<T, E>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let attempts = policy.max_attempts.max(1);
    let mut delay_ms = policy.initial_backoff_ms.max(1);

    for attempt in 1..=attempts {
        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(error) if attempt == attempts => return Err(error),
            Err(_) => {
                sleep(Duration::from_millis(delay_ms)).await;
                delay_ms = delay_ms
                    .saturating_mul(2)
                    .min(policy.max_backoff_ms.max(delay_ms));
            }
        }
    }

    unreachable!("retry_async always executes at least one attempt")
}
