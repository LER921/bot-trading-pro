use crate::models::{
    AppConfig, BinanceConfig, CalibrationConfig, DashboardConfig, LiveConfig, PairsConfig,
    RiskConfig, RuntimeConfig,
};
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use std::{fs, path::Path};

#[derive(Debug, Clone, serde::Deserialize)]
struct BaseFileConfig {
    runtime: RuntimeConfig,
    binance: BinanceConfig,
}

fn load_toml_file<T>(path: &Path) -> Result<T>
where
    T: DeserializeOwned,
{
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;

    toml::from_str(&contents)
        .with_context(|| format!("failed to parse TOML config {}", path.display()))
}

fn load_optional_toml_file<T>(path: &Path) -> Result<T>
where
    T: DeserializeOwned + Default,
{
    if path.exists() {
        load_toml_file(path)
    } else {
        Ok(T::default())
    }
}

pub fn load_from_dir(path: impl AsRef<Path>) -> Result<AppConfig> {
    let root = path.as_ref();
    let base: BaseFileConfig = load_toml_file(&root.join("base.toml"))?;
    let live: LiveConfig = load_toml_file(&root.join("live.toml"))?;
    let pairs: PairsConfig = load_toml_file(&root.join("pairs.toml"))?;
    let risk: RiskConfig = load_toml_file(&root.join("risk.toml"))?;
    let calibration: CalibrationConfig =
        load_optional_toml_file(&root.join("calibration.toml"))?;
    let dashboard: DashboardConfig = load_toml_file(&root.join("dashboard.toml"))?;

    let config = AppConfig {
        runtime: base.runtime,
        binance: base.binance,
        live,
        pairs,
        risk,
        calibration,
        dashboard,
    };
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir() -> std::path::PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("bot_trading_config_loader_{suffix}"))
    }

    #[test]
    fn load_from_dir_uses_default_calibration_when_file_is_missing() {
        let root = unique_temp_dir();
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("base.toml"),
            r#"[runtime]
environment = "test"
state_dir = "var/state"
event_log_dir = "var/log"
bootstrap_cancel_open_orders = true

[binance]
rest_base_url = "https://api.binance.com"
market_ws_url = "wss://stream.binance.com:9443/stream"
user_ws_url = "wss://stream.binance.com:9443/ws"
api_key_env = "BINANCE_API_KEY"
api_secret_env = "BINANCE_API_SECRET"
recv_window_ms = 5000
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("live.toml"),
            r#"trading_enabled = false
start_in_paused_mode = true
allow_market_orders = false
cancel_all_on_risk_off = true
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("pairs.toml"),
            r#"[[pairs]]
symbol = "BTCUSDC"
enabled = true
max_quote_notional = "3450"
max_inventory_base = "0.030"
soft_inventory_base = "0.020"
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("risk.toml"),
            r#"max_daily_loss_usdc = "120"
max_symbol_drawdown_usdc = "60"
max_open_orders_per_symbol = 6
stale_market_data_ms = 1500
stale_account_events_ms = 5000
max_clock_drift_ms = 250

[inventory]
quote_skew_bps_per_inventory_unit = "8"
neutralization_clip_fraction = "0.20"
reduce_only_trigger_ratio = "0.90"
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("dashboard.toml"),
            r#"bind_address = "127.0.0.1:8080"
enable_write_routes = false
"#,
        )
        .unwrap();

        let config = load_from_dir(&root).unwrap();

        assert_eq!(config.calibration.execution.loop_interval_ms, 1_000);
        assert_eq!(config.calibration.market_making.quote_ttl_secs, 5);
        assert_eq!(config.calibration.scalping.quote_ttl_secs, 2);
        assert_eq!(
            config.calibration.execution.local_min_notional_quote,
            common::Decimal::from_str_exact("10.00").unwrap()
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_from_dir_supports_partial_calibration_override() {
        let root = unique_temp_dir();
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("base.toml"),
            r#"[runtime]
environment = "test"
state_dir = "var/state"
event_log_dir = "var/log"
bootstrap_cancel_open_orders = true

[binance]
rest_base_url = "https://api.binance.com"
market_ws_url = "wss://stream.binance.com:9443/stream"
user_ws_url = "wss://stream.binance.com:9443/ws"
api_key_env = "BINANCE_API_KEY"
api_secret_env = "BINANCE_API_SECRET"
recv_window_ms = 5000
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("live.toml"),
            r#"trading_enabled = false
start_in_paused_mode = true
allow_market_orders = false
cancel_all_on_risk_off = true
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("pairs.toml"),
            r#"[[pairs]]
symbol = "BTCUSDC"
enabled = true
max_quote_notional = "3450"
max_inventory_base = "0.030"
soft_inventory_base = "0.020"
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("risk.toml"),
            r#"max_daily_loss_usdc = "120"
max_symbol_drawdown_usdc = "60"
max_open_orders_per_symbol = 6
max_reject_rate = "0.20"
stale_market_data_ms = 1500
stale_account_events_ms = 5000
max_clock_drift_ms = 250

[inventory]
quote_skew_bps_per_inventory_unit = "8"
neutralization_clip_fraction = "0.20"
reduce_only_trigger_ratio = "0.90"
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("calibration.toml"),
            r#"[execution]
loop_interval_ms = 800
"#,
        )
        .unwrap();
        std::fs::write(
            root.join("dashboard.toml"),
            r#"bind_address = "127.0.0.1:8080"
enable_write_routes = false
"#,
        )
        .unwrap();

        let config = load_from_dir(&root).unwrap();

        assert_eq!(config.calibration.execution.loop_interval_ms, 800);
        assert_eq!(config.calibration.execution.stale_order_cancel_ms, 6_000);
        assert_eq!(
            config.calibration.execution.local_min_notional_quote,
            common::Decimal::from_str_exact("10.00").unwrap()
        );
        assert_eq!(
            config.calibration.selection.scalp_activation_flow_imbalance,
            common::Decimal::from_str_exact("0.20").unwrap()
        );
        assert_eq!(config.calibration.market_making.quote_ttl_secs, 5);

        std::fs::remove_dir_all(root).unwrap();
    }
}
