use anyhow::{ensure, Result};
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
    pub calibration: CalibrationConfig,
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

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.enabled_symbols().is_empty(),
            "config must enable at least one trading pair"
        );
        ensure!(
            !self.runtime.state_dir.trim().is_empty(),
            "runtime.state_dir must not be empty"
        );
        ensure!(
            !self.runtime.event_log_dir.trim().is_empty(),
            "runtime.event_log_dir must not be empty"
        );
        ensure!(
            self.binance.recv_window_ms > 0,
            "binance.recv_window_ms must be > 0"
        );
        ensure!(
            self.risk.max_open_orders_per_symbol >= 2,
            "risk.max_open_orders_per_symbol must be >= 2 to support two-sided passive quoting"
        );
        ensure_positive_decimal("risk.max_daily_loss_usdc", self.risk.max_daily_loss_usdc)?;
        ensure_positive_decimal(
            "risk.max_symbol_drawdown_usdc",
            self.risk.max_symbol_drawdown_usdc,
        )?;
        ensure_fraction(
            "risk.max_reject_rate",
            self.risk.max_reject_rate,
            false,
            true,
        )?;
        ensure!(
            self.risk.stale_market_data_ms > 0,
            "risk.stale_market_data_ms must be > 0"
        );
        ensure!(
            self.risk.stale_account_events_ms >= self.risk.stale_market_data_ms,
            "risk.stale_account_events_ms must be >= risk.stale_market_data_ms"
        );
        ensure!(
            self.risk.max_clock_drift_ms > 0,
            "risk.max_clock_drift_ms must be > 0"
        );

        ensure_positive_decimal(
            "risk.inventory.quote_skew_bps_per_inventory_unit",
            self.risk.inventory.quote_skew_bps_per_inventory_unit,
        )?;
        ensure_fraction(
            "risk.inventory.neutralization_clip_fraction",
            self.risk.inventory.neutralization_clip_fraction,
            false,
            true,
        )?;
        ensure_fraction(
            "risk.inventory.reduce_only_trigger_ratio",
            self.risk.inventory.reduce_only_trigger_ratio,
            false,
            true,
        )?;

        for pair in self.pairs.pairs.iter().filter(|pair| pair.enabled) {
            ensure_positive_decimal(
                &format!("pairs.{}.max_quote_notional", pair.symbol),
                pair.max_quote_notional,
            )?;
            ensure_positive_decimal(
                &format!("pairs.{}.max_inventory_base", pair.symbol),
                pair.max_inventory_base,
            )?;
            ensure_positive_decimal(
                &format!("pairs.{}.soft_inventory_base", pair.symbol),
                pair.soft_inventory_base,
            )?;
            ensure!(
                pair.soft_inventory_base < pair.max_inventory_base,
                "pairs.{} soft_inventory_base must stay below max_inventory_base",
                pair.symbol
            );

            let reduce_only_inventory_base =
                pair.max_inventory_base * self.risk.inventory.reduce_only_trigger_ratio;
            ensure!(
                reduce_only_inventory_base > pair.soft_inventory_base,
                "pairs.{} reduce-only trigger inventory must stay above soft inventory",
                pair.symbol
            );
        }

        let execution = &self.calibration.execution;
        ensure!(
            execution.loop_interval_ms > 0,
            "calibration.execution.loop_interval_ms must be > 0"
        );
        ensure!(
            execution.stale_order_cancel_ms > 0,
            "calibration.execution.stale_order_cancel_ms must be > 0"
        );
        ensure!(
            execution.bootstrap_trade_seed_limit > 0,
            "calibration.execution.bootstrap_trade_seed_limit must be > 0"
        );
        ensure!(
            execution.fill_recovery_fetch_limit > 0,
            "calibration.execution.fill_recovery_fetch_limit must be > 0"
        );
        ensure_positive_decimal(
            "calibration.execution.local_min_notional_quote",
            execution.local_min_notional_quote,
        )?;
        ensure!(
            self.risk.stale_market_data_ms > execution.loop_interval_ms,
            "risk.stale_market_data_ms must stay above calibration.execution.loop_interval_ms"
        );

        let market_making = &self.calibration.market_making;
        ensure_positive_decimal(
            "calibration.market_making.maker_fee_bps",
            market_making.maker_fee_bps,
        )?;
        ensure_positive_decimal(
            "calibration.market_making.slippage_buffer_bps",
            market_making.slippage_buffer_bps,
        )?;
        ensure_positive_decimal(
            "calibration.market_making.min_net_edge_bps",
            market_making.min_net_edge_bps,
        )?;
        ensure_positive_decimal(
            "calibration.market_making.min_market_spread_bps",
            market_making.min_market_spread_bps,
        )?;
        ensure_fraction(
            "calibration.market_making.max_toxicity_score",
            market_making.max_toxicity_score,
            false,
            true,
        )?;
        ensure_positive_decimal(
            "calibration.market_making.base_skew_bps",
            market_making.base_skew_bps,
        )?;
        ensure_positive_decimal(
            "calibration.market_making.momentum_skew_weight",
            market_making.momentum_skew_weight,
        )?;
        ensure_positive_decimal(
            "calibration.market_making.volatility_widening_weight",
            market_making.volatility_widening_weight,
        )?;
        ensure_fraction(
            "calibration.market_making.quote_size_fraction",
            market_making.quote_size_fraction,
            false,
            true,
        )?;
        ensure!(
            market_making.quote_ttl_secs > 0,
            "calibration.market_making.quote_ttl_secs must be > 0"
        );
        ensure!(
            market_making.min_market_spread_bps <= dec("5.0"),
            "calibration.market_making.min_market_spread_bps must stay in a realistic live gating range"
        );

        let scalping = &self.calibration.scalping;
        ensure_positive_decimal(
            "calibration.scalping.taker_fee_bps",
            scalping.taker_fee_bps,
        )?;
        ensure_positive_decimal(
            "calibration.scalping.slippage_buffer_bps",
            scalping.slippage_buffer_bps,
        )?;
        ensure_positive_decimal(
            "calibration.scalping.min_entry_score",
            scalping.min_entry_score,
        )?;
        ensure_positive_decimal(
            "calibration.scalping.min_momentum_bps",
            scalping.min_momentum_bps,
        )?;
        ensure_fraction(
            "calibration.scalping.max_toxicity_score",
            scalping.max_toxicity_score,
            false,
            true,
        )?;
        ensure_fraction(
            "calibration.scalping.max_position_fraction",
            scalping.max_position_fraction,
            false,
            true,
        )?;
        ensure_fraction(
            "calibration.scalping.quote_size_fraction",
            scalping.quote_size_fraction,
            false,
            true,
        )?;
        ensure!(
            scalping.quote_ttl_secs > 0,
            "calibration.scalping.quote_ttl_secs must be > 0"
        );
        ensure!(
            scalping.max_position_fraction < self.risk.inventory.reduce_only_trigger_ratio,
            "calibration.scalping.max_position_fraction must stay below risk.inventory.reduce_only_trigger_ratio"
        );
        ensure!(
            scalping.quote_size_fraction <= market_making.quote_size_fraction,
            "calibration.scalping.quote_size_fraction must stay <= calibration.market_making.quote_size_fraction"
        );
        ensure!(
            scalping.max_toxicity_score <= market_making.max_toxicity_score + dec("0.15"),
            "calibration.scalping.max_toxicity_score must not exceed calibration.market_making.max_toxicity_score by more than 0.15"
        );

        let selection = &self.calibration.selection;
        ensure_positive_decimal(
            "calibration.selection.scalp_activation_momentum_bps",
            selection.scalp_activation_momentum_bps,
        )?;
        ensure_fraction(
            "calibration.selection.scalp_activation_flow_imbalance",
            selection.scalp_activation_flow_imbalance,
            false,
            true,
        )?;
        ensure!(
            selection.scalp_activation_momentum_bps >= scalping.min_momentum_bps,
            "calibration.selection.scalp_activation_momentum_bps must stay >= calibration.scalping.min_momentum_bps"
        );

        let max_quote_ttl_ms = market_making.quote_ttl_secs.max(scalping.quote_ttl_secs) * 1_000;
        let min_quote_ttl_ms = market_making.quote_ttl_secs.min(scalping.quote_ttl_secs) * 1_000;
        ensure!(
            execution.stale_order_cancel_ms
                >= max_quote_ttl_ms
                    + i64::try_from(execution.loop_interval_ms).unwrap_or(i64::MAX),
            "calibration.execution.stale_order_cancel_ms must stay >= the longest strategy quote TTL plus one execution loop"
        );
        ensure!(
            i64::try_from(execution.loop_interval_ms).unwrap_or(i64::MAX) <= min_quote_ttl_ms,
            "calibration.execution.loop_interval_ms must stay <= the shortest strategy quote TTL"
        );

        Ok(())
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
                quote_skew_bps_per_inventory_unit: self
                    .risk
                    .inventory
                    .quote_skew_bps_per_inventory_unit,
                neutralization_clip_fraction: self
                    .risk
                    .inventory
                    .neutralization_clip_fraction,
                reduce_only_trigger_ratio: self.risk.inventory.reduce_only_trigger_ratio,
            },
            max_daily_loss_usdc: self.risk.max_daily_loss_usdc,
            max_symbol_drawdown_usdc: self.risk.max_symbol_drawdown_usdc,
            max_total_exposure_quote,
            max_reject_rate: self.risk.max_reject_rate,
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
    #[serde(default = "default_max_reject_rate")]
    pub max_reject_rate: Decimal,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CalibrationConfig {
    #[serde(default)]
    pub execution: ExecutionCalibrationConfig,
    #[serde(default)]
    pub selection: StrategySelectionCalibrationConfig,
    #[serde(default)]
    pub market_making: MarketMakingCalibrationConfig,
    #[serde(default)]
    pub scalping: ScalpingCalibrationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionCalibrationConfig {
    pub loop_interval_ms: u64,
    pub stale_order_cancel_ms: i64,
    pub bootstrap_trade_seed_limit: usize,
    pub fill_recovery_fetch_limit: usize,
    pub local_min_notional_quote: Decimal,
}

impl Default for ExecutionCalibrationConfig {
    fn default() -> Self {
        Self {
            loop_interval_ms: 1_000,
            stale_order_cancel_ms: 6_000,
            bootstrap_trade_seed_limit: 50,
            fill_recovery_fetch_limit: 50,
            local_min_notional_quote: dec("10.00"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategySelectionCalibrationConfig {
    pub scalp_activation_momentum_bps: Decimal,
    pub scalp_activation_flow_imbalance: Decimal,
}

impl Default for StrategySelectionCalibrationConfig {
    fn default() -> Self {
        Self {
            scalp_activation_momentum_bps: dec("8"),
            scalp_activation_flow_imbalance: dec("0.20"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MarketMakingCalibrationConfig {
    pub maker_fee_bps: Decimal,
    pub slippage_buffer_bps: Decimal,
    pub min_net_edge_bps: Decimal,
    pub min_market_spread_bps: Decimal,
    pub max_toxicity_score: Decimal,
    pub base_skew_bps: Decimal,
    pub momentum_skew_weight: Decimal,
    pub volatility_widening_weight: Decimal,
    pub quote_size_fraction: Decimal,
    pub quote_ttl_secs: i64,
}

impl Default for MarketMakingCalibrationConfig {
    fn default() -> Self {
        Self {
            maker_fee_bps: dec("0.75"),
            slippage_buffer_bps: dec("0.25"),
            min_net_edge_bps: dec("1.00"),
            min_market_spread_bps: dec("1.00"),
            max_toxicity_score: dec("0.70"),
            base_skew_bps: dec("8"),
            momentum_skew_weight: dec("0.25"),
            volatility_widening_weight: dec("0.30"),
            quote_size_fraction: dec("0.12"),
            quote_ttl_secs: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScalpingCalibrationConfig {
    pub taker_fee_bps: Decimal,
    pub slippage_buffer_bps: Decimal,
    pub min_entry_score: Decimal,
    pub min_momentum_bps: Decimal,
    pub max_toxicity_score: Decimal,
    pub max_position_fraction: Decimal,
    pub quote_size_fraction: Decimal,
    pub quote_ttl_secs: i64,
}

impl Default for ScalpingCalibrationConfig {
    fn default() -> Self {
        Self {
            taker_fee_bps: dec("1.00"),
            slippage_buffer_bps: dec("0.50"),
            min_entry_score: dec("6"),
            min_momentum_bps: dec("4"),
            max_toxicity_score: dec("0.80"),
            max_position_fraction: dec("0.40"),
            quote_size_fraction: dec("0.08"),
            quote_ttl_secs: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardConfig {
    pub bind_address: String,
    pub enable_write_routes: bool,
}

fn ensure_positive_decimal(name: &str, value: Decimal) -> Result<()> {
    ensure!(value > Decimal::ZERO, "{name} must be > 0");
    Ok(())
}

fn ensure_fraction(name: &str, value: Decimal, allow_zero: bool, allow_one: bool) -> Result<()> {
    let lower_ok = if allow_zero {
        value >= Decimal::ZERO
    } else {
        value > Decimal::ZERO
    };
    let upper_ok = if allow_one {
        value <= Decimal::ONE
    } else {
        value < Decimal::ONE
    };
    ensure!(
        lower_ok && upper_ok,
        "{name} must stay within the configured fractional bounds"
    );
    Ok(())
}

fn default_max_reject_rate() -> Decimal {
    dec("0.20")
}

fn dec(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_app_config() -> AppConfig {
        AppConfig {
            runtime: RuntimeConfig {
                environment: "test".to_string(),
                state_dir: "var/state".to_string(),
                event_log_dir: "var/log".to_string(),
                bootstrap_cancel_open_orders: true,
            },
            binance: BinanceConfig {
                rest_base_url: "https://api.binance.com".to_string(),
                market_ws_url: "wss://stream.binance.com:9443/stream".to_string(),
                user_ws_url: "wss://stream.binance.com:9443/ws".to_string(),
                api_key_env: "BINANCE_API_KEY".to_string(),
                api_secret_env: "BINANCE_API_SECRET".to_string(),
                recv_window_ms: 5_000,
            },
            live: LiveConfig {
                trading_enabled: false,
                start_in_paused_mode: true,
                allow_market_orders: false,
                cancel_all_on_risk_off: true,
            },
            pairs: PairsConfig {
                pairs: vec![PairConfig {
                    symbol: Symbol::BtcUsdc,
                    enabled: true,
                    max_quote_notional: dec("3450"),
                    max_inventory_base: dec("0.030"),
                    soft_inventory_base: dec("0.020"),
                }],
            },
            risk: RiskConfig {
                max_daily_loss_usdc: dec("120"),
                max_symbol_drawdown_usdc: dec("60"),
                max_open_orders_per_symbol: 6,
                max_reject_rate: dec("0.20"),
                stale_market_data_ms: 1_500,
                stale_account_events_ms: 5_000,
                max_clock_drift_ms: 250,
                inventory: InventoryControlConfig {
                    quote_skew_bps_per_inventory_unit: dec("8"),
                    neutralization_clip_fraction: dec("0.20"),
                    reduce_only_trigger_ratio: dec("0.90"),
                },
            },
            calibration: CalibrationConfig::default(),
            dashboard: DashboardConfig {
                bind_address: "127.0.0.1:8080".to_string(),
                enable_write_routes: false,
            },
        }
    }

    #[test]
    fn default_calibration_is_coherent() {
        let config = sample_app_config();

        config.validate().unwrap();
        assert!(
            config.calibration.execution.stale_order_cancel_ms
                >= config.calibration.market_making.quote_ttl_secs * 1_000
        );
        assert_eq!(
            config.calibration.execution.local_min_notional_quote,
            dec("10.00")
        );
        assert!(
            config.calibration.selection.scalp_activation_momentum_bps
                >= config.calibration.scalping.min_momentum_bps
        );
        assert!(config.risk.max_reject_rate <= dec("0.20"));
    }

    #[test]
    fn rejects_incoherent_inventory_thresholds() {
        let mut config = sample_app_config();
        config.pairs.pairs[0].soft_inventory_base = dec("0.029");
        config.risk.inventory.reduce_only_trigger_ratio = dec("0.90");
        config.pairs.pairs[0].max_inventory_base = dec("0.030");

        let error = config.validate().unwrap_err().to_string();

        assert!(error.contains("reduce-only trigger inventory"));
    }

    #[test]
    fn rejects_stale_cancel_shorter_than_quote_ttl() {
        let mut config = sample_app_config();
        config.calibration.execution.stale_order_cancel_ms = 4_000;

        let error = config.validate().unwrap_err().to_string();

        assert!(error.contains("stale_order_cancel_ms"));
    }

    #[test]
    fn allows_tight_market_making_spread_gate_for_major_spot_pairs() {
        let mut config = sample_app_config();
        config.calibration.market_making.min_market_spread_bps = dec("0.001");

        config.validate().unwrap();
    }

    #[test]
    fn rejects_selection_flow_threshold_outside_fraction_bounds() {
        let mut config = sample_app_config();
        config.calibration.selection.scalp_activation_flow_imbalance = dec("1.20");

        let error = config.validate().unwrap_err().to_string();

        assert!(error.contains("scalp_activation_flow_imbalance"));
    }
}
