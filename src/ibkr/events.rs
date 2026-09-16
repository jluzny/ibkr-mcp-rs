//! Wall Street Horizon (WSH) event data — earnings and corporate events.
//!
//! The 2026-09-16 direct probe returned **0 rows** for every filter variant,
//! which almost always means the account has no WSH entitlement. A silent empty
//! list would be indistinguishable from "no events in range", so this module
//! probes entitlement first (`wsh_metadata`) and always reports an explicit
//! `entitlement` + `status` + human-readable `message` alongside the rows.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use ibapi::prelude::*;
use time::Date;
use tracing::{info, warn};

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::IbkrError;
use crate::ibkr::history::map_ibapi_error;

/// Ceiling for each WSH round trip.
const EVENTS_TIMEOUT_SECS: u64 = 30;

/// Outcome of a WSH event query.
///
/// `entitlement` and `status` are deliberately separate:
/// * `entitlement: "ok"` — the WSH metadata service answered.
/// * `status: "ok"` — rows came back. `status: "no_events"` with
///   `entitlement: "unknown"` is the unentitled-account signature.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EventQueryResult {
    /// `"ok"` | `"unavailable"`.
    pub entitlement: String,
    /// `"ok"` | `"no_events"` | `"not_entitled"` | `"unparsed"`.
    pub status: String,
    pub message: String,
    pub contract_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
    pub count: usize,
    pub rows: Vec<serde_json::Value>,
    /// Raw `data_json` when it could not be parsed into rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
}

#[derive(Debug)]
pub struct EventManager {
    client: Arc<IbkrClient>,
}

impl EventManager {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        Self { client }
    }

    /// Resolve a stock symbol to its IBKR contract id.
    pub async fn resolve_conid(&self, symbol: &str) -> Result<i32, IbkrError> {
        let client = self.client.get_client().await?;
        let contract = Contract::stock(symbol).build();

        let details = tokio::time::timeout(
            StdDuration::from_secs(EVENTS_TIMEOUT_SECS),
            client.contract_details(&contract),
        )
        .await
        .map_err(|_| IbkrError::RequestTimeout)?
        .map_err(map_ibapi_error)?;

        details
            .first()
            .map(|d| d.contract.contract_id)
            .ok_or_else(|| {
                IbkrError::InvalidSymbol(format!("no contract details returned for {symbol}"))
            })
    }

    /// Fetch WSH event rows for a contract, reporting entitlement explicitly.
    pub async fn get_events(
        &self,
        symbol: Option<&str>,
        conid: Option<i32>,
        start: Option<Date>,
        end: Option<Date>,
    ) -> Result<EventQueryResult, IbkrError> {
        let contract_id = match (conid, symbol) {
            (Some(c), _) if c > 0 => c,
            (_, Some(s)) => self.resolve_conid(s).await?,
            _ => {
                return Err(IbkrError::InvalidSymbol(
                    "provide either a symbol or a conid".to_string(),
                ))
            }
        };

        let client = self.client.get_client().await?;

        // Entitlement probe. A failure here means the account cannot see the
        // Wall Street Horizon service at all.
        let (entitlement, metadata_note): (String, Option<String>) =
            match tokio::time::timeout(
                StdDuration::from_secs(EVENTS_TIMEOUT_SECS),
                client.wsh_metadata(),
            )
            .await
            {
                Ok(Ok(meta)) => ("ok".to_string(), Some(truncate(&meta.data_json, 200))),
                Ok(Err(e)) => (
                    "unavailable".to_string(),
                    Some(format!("wsh_metadata failed: {}", map_ibapi_error(e))),
                ),
                Err(_) => (
                    "unknown".to_string(),
                    Some("wsh_metadata timed out".to_string()),
                ),
            };

        info!(
            contract_id,
            entitlement = %entitlement,
            "Requesting WSH event data"
        );

        let mut builder = client.wsh_event_data_by_contract(contract_id);
        if let Some(s) = start {
            builder = builder.starting(s);
        }
        if let Some(e) = end {
            builder = builder.ending(e);
        }

        let start_date = start.map(|d| d.to_string());
        let end_date = end.map(|d| d.to_string());

        let fetched = tokio::time::timeout(StdDuration::from_secs(EVENTS_TIMEOUT_SECS), builder.fetch())
            .await
            .map_err(|_| IbkrError::RequestTimeout)?;

        let base = |entitlement: &str, status: &str, message: String, rows: Vec<serde_json::Value>, raw: Option<String>| {
            EventQueryResult {
                entitlement: entitlement.to_string(),
                status: status.to_string(),
                message,
                contract_id,
                start_date: start_date.clone(),
                end_date: end_date.clone(),
                count: rows.len(),
                rows,
                raw,
            }
        };

        match fetched {
            Ok(data) => {
                let payload = data.data_json.trim().to_string();

                if payload.is_empty() {
                    return Ok(base(
                        &entitlement,
                        "no_events",
                        format!(
                            "WSH request succeeded but returned no payload for conid {contract_id}. \
                             Most likely this account has no Wall Street Horizon entitlement \
                             (the 2026-09-16 direct probe got 0 rows for every filter variant), \
                             or there are no events in the requested range. \
                             metadata: {}",
                            metadata_note.unwrap_or_else(|| "n/a".to_string())
                        ),
                        Vec::new(),
                        None,
                    ));
                }

                match parse_rows(&payload) {
                    Ok(rows) if !rows.is_empty() => Ok(base(
                        "ok",
                        "ok",
                        format!("{} WSH event row(s) for conid {contract_id}", rows.len()),
                        rows,
                        None,
                    )),
                    Ok(_) => Ok(base(
                        &entitlement,
                        "no_events",
                        format!(
                            "WSH payload decoded to an empty row list for conid {contract_id} \
                             (no events in the requested range). metadata: {}",
                            metadata_note.unwrap_or_else(|| "n/a".to_string())
                        ),
                        Vec::new(),
                        None,
                    )),
                    Err(e) => {
                        warn!(contract_id, error = %e, "WSH payload was not decodable JSON");
                        Ok(base(
                            "ok",
                            "unparsed",
                            format!("WSH payload for conid {contract_id} was not decodable JSON: {e}"),
                            Vec::new(),
                            Some(truncate(&payload, 2000)),
                        ))
                    }
                }
            }
            Err(e) => {
                let mapped = map_ibapi_error(e);
                Ok(base(
                    "unavailable",
                    "not_entitled",
                    format!(
                        "WSH event data unavailable for conid {contract_id}: {mapped}. \
                         This is the expected result without a Wall Street Horizon subscription. \
                         metadata: {}",
                        metadata_note.unwrap_or_else(|| "n/a".to_string())
                    ),
                    Vec::new(),
                    None,
                ))
            }
        }
    }
}

/// Normalise a `data_json` payload into a row list.
pub fn parse_rows(payload: &str) -> Result<Vec<serde_json::Value>, String> {
    let value: serde_json::Value =
        serde_json::from_str(payload).map_err(|e| format!("invalid JSON: {e}"))?;

    Ok(match value {
        serde_json::Value::Array(rows) => rows,
        serde_json::Value::Null => Vec::new(),
        // A single event object, or an object wrapping a row array.
        other @ serde_json::Value::Object(_) => match other.get("data").cloned() {
            Some(serde_json::Value::Array(rows)) => rows,
            _ => vec![other],
        },
        other => vec![other],
    })
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_an_array_payload() {
        let rows = parse_rows(r#"[{"a":1},{"a":2}]"#).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn parses_a_single_object_payload() {
        let rows = parse_rows(r#"{"eventType":"earnings"}"#).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["eventType"], json!("earnings"));
    }

    #[test]
    fn unwraps_a_data_wrapper_object() {
        let rows = parse_rows(r#"{"data":[{"a":1}]}"#).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn empty_payload_is_empty_not_an_error() {
        assert!(parse_rows("null").unwrap().is_empty());
    }

    #[test]
    fn non_json_is_an_error() {
        assert!(parse_rows("<html>not json</html>").is_err());
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello", 10), "hello");
        let s = truncate("ěščřžýáíé", 4);
        assert!(s.ends_with('…'));
        assert!(s.chars().count() <= 5);
    }
}
