use common::Decimal;
use domain::{InventoryControl, RiskLimits, Symbol, SymbolRiskLimits};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub binance: BinanceConfig,
    pub live: LiveConfig,
    pub pairs: PairsConfig,
    pub risk: RiskConfig,
    pub dashboard: DashboardConfig,
}

impl AppConfig {
    pub fn enabled_symbols(&self) -> Vec<Symbol> {
        self.pairs
            .pairs
            .iter()
            .filter(|pair| pair.enabled)
            .map(|pair| pair.symbol)
            .collect()
    }

    pub fn pair(&self, symbol: Symbol) -> Option<&PairConfig> {
        self.pairs.pairs.iter().find(|pair| pair.symbol == symbol)
    }

    pub fn risk_limits(&self) -> RiskLimits {
        let per_symbol = self
            .pairs
            .pairs
            .iter()
            .filter(|pair| pair.enabled)
            .map(|pair| SymbolRiskLimits {
                symbol: pair.symbol,
                max_inventory_base: pair.max_inventory_base,
                soft_inventory_base: pair.soft_inventory_base,
                max_quote_notional: pair.max_quote_notional,
                max_open_orders: self.risk.max_open_orders_per_symbol,
            })
            .collect::<Vec<_>>();
        let max_total_exposure_quote = per_symbol
            .iter()
            .fold(Decimal::ZERO, |acc, limits| acc + limits.max_quote_notional);

        RiskLimits {
            per_symbol,
            inventory: InventoryControl {
                quote_skew_bps_per_inventory_unit: self.risk.inventory.quote_skew_bps_per_inventory_unit,
                neutralization_clip_fraction: self.risk.inventory.neutralization_clip_fraction,
                reduce_only_trigger_ratio: self.risk.inventory.reduce_only_trigger_ratio,
            },
            max_daily_loss_usdc: self.risk.max_daily_loss_usdc,
            max_symbol_drawdown_usdc: self.risk.max_symbol_drawdown_usdc,
            max_total_exposure_quote,
            max_reject_rate: Decimal::from_str_exact("0.30").expect("static decimal"),
            stale_market_data_ms: self.risk.stale_market_data_ms,
            stale_account_events_ms: self.risk.stale_account_events_ms,
            max_clock_drift_ms: self.risk.max_clock_drift_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub environment: String,
    pub state_dir: String,
    pub event_log_dir: String,
    pub bootstrap_cancel_open_orders: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceConfig {
    pub rest_base_url: String,
    pub market_ws_url: String,
    pub user_ws_url: String,
    pub api_key_env: String,
    pub api_secret_env: String,
    pub recv_window_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveConfig {
    pub trading_enabled: bool,
    pub start_in_paused_mode: bool,
    pub allow_market_orders: bool,
    pub cancel_all_on_risk_off: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairsConfig {
    pub pairs: Vec<PairConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairConfig {
    pub symbol: Symbol,
    pub enabled: bool,
    pub max_quote_notional: Decimal,
    pub max_inventory_base: Decimal,
    pub soft_inventory_base: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_daily_loss_usdc: Decimal,
    pub max_symbol_drawdown_usdc: Decimal,
    pub max_open_orders_per_symbol: u32,
    pub stale_market_data_ms: u64,
    pub stale_account_events_ms: u64,
    pub max_clock_drift_ms: i64,
    pub inventory: InventoryControlConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryControlConfig {
    pub quote_skew_bps_per_inventory_unit: Decimal,
    pub neutralization_clip_fraction: Decimal,
    pub reduce_only_trigger_ratio: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardConfig {
    pub bind_address: String,
    pub enable_write_routes: bool,
}
