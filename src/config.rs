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
    /// Provenance of the formerly-inert D5 flags, resolved by [`Config::load`].
    /// Not a configuration key: never read from a file or the environment.
    #[serde(skip)]
    pub explicit_flags: ExplicitFlags,
}

/// Which formerly-inert flags an operator set explicitly (D5 provenance).
///
/// `ibkr.read_only` and `ibkr.paper_trading` both default to `true` in the
/// built-in config, so taking a built-in default at face value would refuse
/// tools in a default-config run. A flag is therefore live for
/// [`crate::policy::OrderGate`] only when a provider *other than the built-in
/// defaults* supplied it — effectiveness follows provenance
/// (`ibkr-mcp-py/docs/DEVIATIONS.md` §D5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExplicitFlags {
    pub read_only: bool,
    pub paper_trading: bool,
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
                port: 4001,
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
            explicit_flags: ExplicitFlags::default(),
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
        let operator = operator_figment();
        config.explicit_flags = ExplicitFlags {
            read_only: flag_is_explicit(&operator, "ibkr.read_only"),
            paper_trading: flag_is_explicit(&operator, "ibkr.paper_trading"),
        };
        Ok(config)
    }
}

/// The operator layer of [`Config::load`]: the file and environment providers
/// with the built-in defaults left out.
///
/// This is the D5 provenance probe. figment 0.10 keeps per-value provenance in
/// tags with no public accessor, so explicitness is answered structurally: a
/// key that resolves here was supplied by a human or a deployment; a key that
/// resolves only in the merged config came from [`Config::default`] alone and
/// is inert (`ibkr-mcp-py` `config.effective_flag`: explicit AND true refuses).
///
/// It must stay in lockstep with the provider chain in [`Config::load`].
fn operator_figment() -> Figment {
    let env = std::env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
    let env_file = format!("config/{}.yaml", env);

    let mut explicit = Figment::new().merge(Yaml::file("config/default.yaml"));
    if std::path::Path::new(&env_file).exists() {
        explicit = explicit.merge(Yaml::file(&env_file));
    }
    explicit.merge(Env::prefixed("IBKR_MCP_").split("__"))
}

/// Whether an operator provider (config file or environment) supplied the key.
fn flag_is_explicit(explicit: &Figment, path: &str) -> bool {
    explicit.find_value(path).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_load_default() {
        let config = Config::load().expect("should load default config");
        assert_eq!(config.ibkr.port, 4001);
        assert_eq!(config.mcp.port, 8881);
    }

    #[test]
    fn test_config_default_values() {
        let config = Config::default();
        assert_eq!(config.ibkr.port, 4001);
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

    /// The D5 gate takes its flags from provenance: a built-in default is inert
    /// even when it reads `true`, and a file value is live even when it reads
    /// `false` — live here means "an operator spoke", not "the gate refuses".
    #[test]
    fn d5_flags_track_provenance_not_values() {
        // Built-in defaults read true — and still are inert, because the
        // operator layer is empty: nothing was spoken for.
        let defaults = Config::default();
        assert!(defaults.ibkr.read_only);
        assert!(defaults.ibkr.paper_trading);
        let explicit = Figment::new();
        assert!(!flag_is_explicit(&explicit, "ibkr.read_only"));
        assert!(!flag_is_explicit(&explicit, "ibkr.paper_trading"));

        // A file value is live — whatever it says, so `false` is live too.
        let explicit = Figment::new()
            .merge(Yaml::string("ibkr:\n  read_only: false\n  paper_trading: false\n"));
        assert!(flag_is_explicit(&explicit, "ibkr.read_only"));
        assert!(flag_is_explicit(&explicit, "ibkr.paper_trading"));
    }

    /// The shipped `config/default.yaml` declares both flags, so a plain load
    /// sees them as operator-set: `read_only: true` makes `what_if` refuse
    /// until a deployment overrides it (`ibkr-mcp-py/docs/DEVIATIONS.md` §D5
    /// row 2). Pinned so the shipped contract cannot drift silently.
    #[test]
    fn the_shipped_config_makes_the_d5_flags_explicit() {
        let loaded = Config::load().expect("should load default config");
        assert!(
            loaded.explicit_flags.read_only,
            "config/default.yaml declares ibkr.read_only — it must read as explicit"
        );
        assert!(
            loaded.explicit_flags.paper_trading,
            "config/default.yaml declares ibkr.paper_trading — it must read as explicit"
        );

        // Effective gate: the file's read_only: true refuses what_if; its
        // paper_trading: false refuses nothing.
        let gate = crate::policy::OrderGate::from_config(&loaded);
        assert!(gate.read_only);
        assert!(!gate.paper_trading);
        assert!(gate.refuses("what_if", None));
    }
}
