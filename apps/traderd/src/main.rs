use anyhow::Result;
use config_loader::load_from_dir;
use exchange_binance_spot::{BinanceCredentials, BinanceSpotClient};
use traderd::TraderRuntime;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let config = load_from_dir("config")?;
    let credentials = BinanceCredentials::from_env(
        &config.binance.api_key_env,
        &config.binance.api_secret_env,
    )?;
    let gateway = BinanceSpotClient::new(
        &config.binance.rest_base_url,
        &config.binance.market_ws_url,
        &config.binance.user_ws_url,
        config.binance.recv_window_ms,
        credentials,
    )?;

    TraderRuntime::new(config, gateway)?.run_until_shutdown().await
}
