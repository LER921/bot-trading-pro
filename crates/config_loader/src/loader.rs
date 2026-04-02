use crate::models::{
    AppConfig, BinanceConfig, DashboardConfig, LiveConfig, PairsConfig, RiskConfig, RuntimeConfig,
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

pub fn load_from_dir(path: impl AsRef<Path>) -> Result<AppConfig> {
    let root = path.as_ref();
    let base: BaseFileConfig = load_toml_file(&root.join("base.toml"))?;
    let live: LiveConfig = load_toml_file(&root.join("live.toml"))?;
    let pairs: PairsConfig = load_toml_file(&root.join("pairs.toml"))?;
    let risk: RiskConfig = load_toml_file(&root.join("risk.toml"))?;
    let dashboard: DashboardConfig = load_toml_file(&root.join("dashboard.toml"))?;

    Ok(AppConfig {
        runtime: base.runtime,
        binance: base.binance,
        live,
        pairs,
        risk,
        dashboard,
    })
}
