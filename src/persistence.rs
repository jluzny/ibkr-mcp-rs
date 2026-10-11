//! Last-good state persistence.
//!
//! When IBKR becomes unreachable, margin monitors and trading automations
//! should be able to decide for themselves whether to act on a stale snapshot
//! instead of just getting `500`. Persist the last successfully-fetched
//! `AccountInfo` to disk; `/health/last-good` exposes it with its age.
//!
//! Path resolution: `IBKR_MCP_PERSIST_PATH` env var, else
//! `/var/lib/ibkr-mcp/last-good.json`, else `/tmp/ibkr-mcp-last-good.json`
//! (graceful degradation on dev machines without `/var/lib/ibkr-mcp`).

use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::ibkr::account::AccountInfo;

/// In-memory + on-disk representation of the last-known-good snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastGoodState {
    pub account: AccountInfo,
    /// Unix seconds when this snapshot was fetched from IBKR.
    pub fetched_at_unix: u64,
    /// True if the file on disk was loaded from this process (vs. a previous one).
    pub loaded_from_disk: bool,
}

static LAST_GOOD: OnceLock<std::sync::RwLock<Option<LastGoodState>>> = OnceLock::new();

fn store() -> &'static std::sync::RwLock<Option<LastGoodState>> {
    LAST_GOOD.get_or_init(|| std::sync::RwLock::new(None))
}

fn persist_path() -> PathBuf {
    if let Ok(p) = std::env::var("IBKR_MCP_PERSIST_PATH") {
        return PathBuf::from(p);
    }
    let prod = PathBuf::from("/var/lib/ibkr-mcp/last-good.json");
    if prod
        .parent()
        .map(|p| p.exists() || std::fs::create_dir_all(p).is_ok())
        .unwrap_or(false)
    {
        return prod;
    }
    PathBuf::from("/tmp/ibkr-mcp-last-good.json")
}

/// Load last-good state from disk at startup. Non-fatal on any error.
pub fn load_from_disk() {
    let path = persist_path();
    match std::fs::read_to_string(&path) {
        Ok(data) => match serde_json::from_str::<LastGoodState>(&data) {
            Ok(mut state) => {
                state.loaded_from_disk = true;
                info!(path = %path.display(), fetched_at_unix = state.fetched_at_unix,
                      "Loaded last-good IBKR snapshot from disk");
                *store().write().unwrap() = Some(state);
            }
            Err(e) => warn!(path = %path.display(), error = %e, "last-good.json corrupt, ignoring"),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Normal on first run — no action
        }
        Err(e) => warn!(path = %path.display(), error = %e, "Failed to read last-good.json"),
    }
}

/// Update the last-good snapshot in memory and on disk.
/// Called from the summary pump every time fresh data lands.
pub fn record(account: AccountInfo) {
    let state = LastGoodState {
        fetched_at_unix: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        account,
        loaded_from_disk: false,
    };

    *store().write().unwrap() = Some(state.clone());

    if let Ok(json) = serde_json::to_string_pretty(&state) {
        let path = persist_path();
        if let Err(e) = std::fs::write(&path, json) {
            warn!(path = %path.display(), error = %e, "Failed to write last-good.json");
        }
    }
}

/// Read the current last-good snapshot, if any.
pub fn get() -> Option<LastGoodState> {
    store().read().unwrap().clone()
}

/// Age of the last-good snapshot in seconds, if any.
pub fn age_secs() -> Option<u64> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_secs();
    get().map(|s| now.saturating_sub(s.fetched_at_unix))
}

/// Phase 0 fault-matrix coverage (PLAN-gateway-supervisor-refactor.md §6.2),
/// case M6: startup must survive a damaged or clock-shifted snapshot file.
///
/// One test on purpose: it mutates the process-wide `IBKR_MCP_PERSIST_PATH`
/// variable, so it must not race a sibling test doing the same thing.
#[cfg(test)]
mod fault_matrix_phase0 {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn account_json(account_id: &str) -> String {
        format!(
            "{{\"account_id\":\"{account_id}\",\"net_liquidation\":1.0,\
             \"available_funds\":1.0,\"excess_liquidity\":1.0,\"buying_power\":1.0,\
             \"currency\":\"USD\",\"daily_pnl\":0.0,\"unrealized_pnl\":0.0,\
             \"realized_pnl\":0.0}}"
        )
    }

    #[test]
    fn m6_damaged_and_clock_shifted_snapshots_are_survivable() {
        let dir = std::env::temp_dir()
            .join(format!("ibkr-mcp-persist-faultmatrix-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create fixture dir");
        let path = dir.join("last-good.json");
        std::env::set_var("IBKR_MCP_PERSIST_PATH", &path);

        // (a) truncated garbage: must be ignored, never fatal.
        std::fs::write(&path, "{ this is not json").expect("write corrupt snapshot");
        *store().write().unwrap() = None;
        load_from_disk();
        assert!(
            get().is_none(),
            "a corrupt snapshot must be ignored, not loaded"
        );

        // (b) valid JSON with a block of fields missing: also ignored.
        std::fs::write(&path, "{\"fetched_at_unix\": 1}").expect("write partial snapshot");
        *store().write().unwrap() = None;
        load_from_disk();
        assert!(
            get().is_none(),
            "a schema-incomplete snapshot must be ignored, not loaded"
        );

        // (c) a snapshot dated in the future (clock moved back): it is still a
        // usable last-known-good, and its age must saturate at 0 hours rather
        // than underflow.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs();
        let future = now + 3600;
        std::fs::write(
            &path,
            format!(
                "{{\"account\":{},\"fetched_at_unix\":{},\"loaded_from_disk\":false}}",
                account_json("DU0000000"),
                future
            ),
        )
        .expect("write future-dated snapshot");
        *store().write().unwrap() = None;
        load_from_disk();
        let state = get().expect("a future-dated snapshot must still load");
        assert_eq!(state.fetched_at_unix, future);
        assert!(state.loaded_from_disk, "a disk snapshot is flagged as such");
        assert_eq!(
            age_secs(),
            Some(0),
            "a future-dated snapshot must report age 0, never underflow"
        );

        // (d) the path is a directory, not a file: still non-fatal.
        std::env::set_var("IBKR_MCP_PERSIST_PATH", &dir);
        *store().write().unwrap() = None;
        load_from_disk();
        assert!(get().is_none(), "an unreadable path must not abort startup");

        std::env::remove_var("IBKR_MCP_PERSIST_PATH");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
