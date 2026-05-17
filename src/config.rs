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
    pub max_cache_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            ibkr: IbkrConfig {
                host: "127.0.0.1".to_string(),
                port: 4003,
                client_id: 100,
                paper_trading: true,
                read_only: true,
                connection_timeout_secs: 10,
                retry_attempts: 100,
                retry_delay_ms: 500,
            },
            mcp: McpConfig {
                host: "0.0.0.0".to_string(),
                port: 8881,
                session_timeout_secs: 0,
            },
            market_data: MarketDataConfig {
                real_time_ttl_secs: 5,
                delayed_ttl_secs: 60,
                max_cache_entries: 1000,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
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

        let config: Config = figment.extract()?;
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
}
