//! Configuration management for ibkr-mcp-rs.
//!
//! Uses [`figment`](https://docs.rs/figment) to layer configuration sources:
//!
//! 1. Environment variables prefixed with `IBKR_MCP__`
//! 2. `config/{env}.yaml` (where `env` defaults to `development`)
//! 3. `config/default.yaml`
//! 4. Built-in defaults
//!
//! Example environment overrides:
//! ```bash
//! IBKR_MCP__IBKR__HOST=192.168.1.10 IBKR_MCP__MCP__PORT=9000 ./ibkr-mcp-rs
//! ```

use figment::{
    providers::{Env, Format, Serialized, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub ibkr: IbkrConfig,
    pub mcp: McpConfig,
    pub market_data: MarketDataConfig,
    pub logging: LoggingConfig,
    pub health: HealthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IbkrConfig {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
    pub paper_trading: bool,
    pub read_only: bool,
    pub connection_timeout_secs: u64,
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpConfig {
    pub host: String,
    pub port: u16,
    pub session_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataConfig {
    pub real_time_ttl_secs: u64,
    pub delayed_ttl_secs: u64,
    pub frozen_ttl_secs: u64,
    pub max_cache_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

/// Health / tool-authorization contract version (PLAN-gateway-supervisor-refactor
/// §6.1).
///
/// `legacy-v0` = the pre-existing tool-authorization semantics with no readiness
/// lease; `lease-v1` is staged and qualified in Phase 2 and never acts as a
/// fallback for a missing, expired or unknown lease.
///
/// The deployed tuple declares this value three times and all three must agree:
/// `health.contract_version` here, `IBKR_MCP_HEALTH__CONTRACT_VERSION` in the
/// systemd unit, and `health_contract_version` in the artifact manifest
/// (`ibkr-stack/scripts/check-mcp-policy.sh` fails the tuple on a mismatch). The
/// process reports the resolved value on `/version`, `/health/live` and
/// `/health/ready` as `contractVersion`.
///
/// The serde default exists only so that dev runs, the fault-matrix scratch
/// instances and library users that pass no config file still start; a deployed
/// config declares the value explicitly (the tuple gate rejects one that does
/// not).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    #[serde(default = "default_contract_version")]
    pub contract_version: String,
}

/// The staged Phase-1 contract version (see [`HealthConfig`]).
pub fn default_contract_version() -> String {
    "legacy-v0".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            ibkr: IbkrConfig {
                host: "127.0.0.1".to_string(),
                port: 4003,
                client_id: 0,
                paper_trading: true,
                read_only: true,
                connection_timeout_secs: 15,
                retry_attempts: 100,
                retry_delay_ms: 1000,
            },
            mcp: McpConfig {
                host: "0.0.0.0".to_string(),
                port: 8881,
                session_timeout_secs: 0,
            },
            market_data: MarketDataConfig {
                real_time_ttl_secs: 5,
                delayed_ttl_secs: 60,
                frozen_ttl_secs: 300,
                max_cache_entries: 1000,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
            },
            health: HealthConfig {
                contract_version: default_contract_version(),
            },
        }
    }
}

impl Config {
    /// Load configuration from files and environment variables.
    /// Priority (highest first): env vars > config/{env}.yaml > config/default.yaml
    pub fn load() -> anyhow::Result<Self> {
        let env = std::env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
        let env_file = format!("config/{}.yaml", env);

        // Start with default values so the binary works without any config files
        let mut figment = Figment::new()
            .merge(Serialized::defaults(Config::default()))
            .merge(Yaml::file("config/default.yaml"));

        if std::path::Path::new(&env_file).exists() {
            figment = figment.merge(Yaml::file(&env_file));
        }

        figment = figment.merge(Env::prefixed("IBKR_MCP_").split("__"));

        let mut config: Config = figment.extract()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_load_default() {
        let config = Config::load().expect("should load default config");
        assert_eq!(config.ibkr.port, 4003);
        assert_eq!(config.mcp.port, 8881);
    }

    #[test]
    fn test_config_default_values() {
        let config = Config::default();
        assert_eq!(config.ibkr.port, 4003);
        assert_eq!(config.mcp.port, 8881);
        assert_eq!(config.ibkr.host, "127.0.0.1");
        assert_eq!(config.mcp.host, "0.0.0.0");
    }

    /// The contract version is a tuple member (PLAN §6.1): it resolves to the
    /// staged `legacy-v0` whether it comes from `config/default.yaml` or from the
    /// built-in default, and `Config::load()` succeeds with the deployed config —
    /// a tuple that declares the wrong or no version is refused by the pre-install
    /// gate, not silently defaulted at runtime.
    #[test]
    fn test_contract_version_resolves() {
        let config = Config::default();
        assert_eq!(config.health.contract_version, default_contract_version());
        assert_eq!(config.health.contract_version, "legacy-v0");

        let loaded = Config::load().expect("should load default config");
        assert_eq!(loaded.health.contract_version, "legacy-v0");
    }
}
