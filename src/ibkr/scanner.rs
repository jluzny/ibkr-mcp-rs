//! IBKR market-scanner access.
//!
//! Read-only: the scanner only ranks symbols, it never trades. Two IBKR limits
//! are enforced in code rather than documented and hoped for:
//!
//! * **≤50 rows per scan** — `rows` is clamped, IBKR rejects more.
//! * **≤10 concurrent scans** — a process-wide [`Semaphore`] held for the
//!   lifetime of each scan. A failed acquire returns a clear error instead of
//!   queueing an MCP session indefinitely behind a full scanner pool.
//!
//! Scanner rows carry **no** market-data fields (no bid/ask/last) — IBKR does
//! not send them on this stream. Callers must fetch quotes separately; the tool
//! response says so explicitly so no consumer assumes otherwise.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use futures::StreamExt;
use ibapi::contracts::TagValue;
use ibapi::prelude::*;
use ibapi::scanner::{ScannerData, ScannerSubscription};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::{is_entitlement_error, IbkrError};
use crate::ibkr::history::map_ibapi_error;

/// IBKR maximum rows per scanner request.
pub const MAX_SCANNER_ROWS: i32 = 50;
/// IBKR maximum concurrent scanner subscriptions per client.
pub const MAX_CONCURRENT_SCANS: usize = 10;
/// Wall-clock ceiling to establish the scanner subscription.
const SCAN_TIMEOUT_SECS: u64 = 20;
/// Idle ceiling between scanner batches. IBKR can take >6s to produce the
/// first batch for large scans (MOST_ACTIVE at 50 rows was observed returning
/// nothing inside 6s), so this is deliberately generous.
const SCAN_IDLE_SECS: u64 = 15;
/// Hard ceiling for a single scan, however many batches arrive.
const SCAN_DEADLINE_SECS: u64 = 45;

/// One ranked scanner result.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScannerRow {
    /// Ranking position within the scan.
    pub rank: i32,
    pub symbol: String,
    pub contract_id: i32,
    pub security_type: String,
    pub currency: String,
    pub exchange: String,
    pub primary_exchange: String,
    pub local_symbol: String,
    /// Combo legs — populated for `NATCOMB` scans as `conid|ratio,conid|ratio`.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub leg: String,
}

/// The result of a scan, including anything the scanner told us along the way.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanResult {
    pub rows: Vec<ScannerRow>,
    /// Non-fatal IBKR notices (e.g. "scan needs a market data subscription").
    pub notices: Vec<String>,
}

#[derive(Debug)]
pub struct ScannerManager {
    client: Arc<IbkrClient>,
    slots: Arc<Semaphore>,
}

impl ScannerManager {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        Self {
            client,
            slots: Arc::new(Semaphore::new(MAX_CONCURRENT_SCANS)),
        }
    }

    /// Run a scanner subscription and return the ranked rows.
    pub async fn run_scan(
        &self,
        scan_code: &str,
        instrument: &str,
        location: &str,
        rows: i32,
        tag_filters: &HashMap<String, String>,
    ) -> Result<ScanResult, IbkrError> {
        let rows = clamp_rows(rows);

        // IBKR caps concurrent scans; the permit lives as long as the
        // subscription below (dropped at the end of this function).
        let _permit = self.slots.clone().try_acquire_owned().map_err(|_| {
            IbkrError::Unknown(format!(
                "{MAX_CONCURRENT_SCANS} concurrent scanner requests already in flight \
                 (IBKR limit) — retry shortly"
            ))
        })?;

        let client = self.client.get_client().await?;

        let params = ScannerSubscription {
            number_of_rows: rows,
            instrument: Some(instrument.to_string()),
            location_code: Some(location.to_string()),
            scan_code: Some(scan_code.to_string()),
            ..Default::default()
        };

        let tags = to_tag_values(tag_filters);

        info!(
            scan_code = %scan_code,
            instrument = %instrument,
            location = %location,
            rows,
            filters = tags.len(),
            "Submitting market scanner request"
        );

        let mut subscription = tokio::time::timeout(
            StdDuration::from_secs(SCAN_TIMEOUT_SECS),
            client.scanner_subscription(&params, &tags),
        )
        .await
        .map_err(|_| IbkrError::RequestTimeout)?
        .map_err(map_ibapi_error)?;

        let mut out: Vec<ScannerRow> = Vec::new();
        let mut notices: Vec<String> = Vec::new();
        let started = std::time::Instant::now();

        loop {
            if started.elapsed() > StdDuration::from_secs(SCAN_DEADLINE_SECS) {
                notices.push(format!(
                    "scan deadline of {SCAN_DEADLINE_SECS}s reached — returning {} row(s)",
                    out.len()
                ));
                break;
            }

            match tokio::time::timeout(StdDuration::from_secs(SCAN_IDLE_SECS), subscription.next())
                .await
            {
                Ok(Some(Ok(SubscriptionItem::Data(batch)))) => {
                    for row in batch {
                        out.push(scanner_row(row));
                    }
                    if out.len() >= rows as usize {
                        break;
                    }
                }
                Ok(Some(Ok(SubscriptionItem::Notice(n)))) => {
                    if is_entitlement_error(n.code) {
                        subscription.cancel().await;
                        return Err(IbkrError::MarketDataSubscriptionRequired {
                            code: n.code,
                            message: n.message,
                        });
                    }
                    notices.push(format!("[{}] {}", n.code, n.message));
                }
                Ok(Some(Err(e))) => {
                    subscription.cancel().await;
                    return Err(map_ibapi_error(e));
                }
                Ok(None) => break,
                // Idle — IBKR has sent everything it is going to send (or the
                // scan is slower than the idle ceiling). Say which, never
                // return a bare empty list.
                Err(_) => {
                    notices.push(format!(
                        "no scanner batch within {SCAN_IDLE_SECS}s — returning {} row(s)",
                        out.len()
                    ));
                    break;
                }
            }
        }

        // Never leak a scanner slot on the gateway.
        subscription.cancel().await;

        out.truncate(rows as usize);
        out.sort_by_key(|r| r.rank);

        if out.is_empty() {
            warn!(scan_code = %scan_code, "Scanner returned no rows");
            notices.push(
                "scanner returned no rows — the scan code may need a market-data \
                 subscription, or IBKR had nothing to rank for this scan/filter"
                    .to_string(),
            );
        }

        Ok(ScanResult { rows: out, notices })
    }
}

/// Clamp a requested row count into IBKR's accepted range.
pub fn clamp_rows(rows: i32) -> i32 {
    rows.clamp(1, MAX_SCANNER_ROWS)
}

/// Convert the tool's `tag_filters` map into IBKR `TagValue`s, sorted for
/// deterministic request bytes.
pub fn to_tag_values(filters: &HashMap<String, String>) -> Vec<TagValue> {
    let mut keyed: Vec<(&String, &String)> = filters.iter().collect();
    keyed.sort_by(|a, b| a.0.cmp(b.0));
    keyed
        .into_iter()
        .map(|(k, v)| TagValue {
            tag: k.clone(),
            value: v.clone(),
        })
        .collect()
}

fn scanner_row(row: ScannerData) -> ScannerRow {
    let contract = row.contract_details.contract;
    ScannerRow {
        rank: row.rank,
        symbol: contract.symbol.to_string(),
        contract_id: contract.contract_id,
        security_type: contract.security_type.to_string(),
        currency: contract.currency.to_string(),
        exchange: contract.exchange.to_string(),
        primary_exchange: contract.primary_exchange.to_string(),
        local_symbol: contract.local_symbol,
        leg: row.leg,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_are_clamped_to_ibkr_limits() {
        assert_eq!(clamp_rows(0), 1);
        assert_eq!(clamp_rows(-5), 1);
        assert_eq!(clamp_rows(25), 25);
        assert_eq!(clamp_rows(50), 50);
        assert_eq!(clamp_rows(500), 50);
    }

    #[test]
    fn tag_filters_become_sorted_tag_values() {
        let mut filters = HashMap::new();
        filters.insert("underConID".to_string(), "12345".to_string());
        filters.insert("Annual".to_string(), "true".to_string());

        let tags = to_tag_values(&filters);
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].tag, "Annual");
        assert_eq!(tags[0].value, "true");
        assert_eq!(tags[1].tag, "underConID");
        assert_eq!(tags[1].value, "12345");
    }

    #[test]
    fn empty_filters_produce_no_tags() {
        assert!(to_tag_values(&HashMap::new()).is_empty());
    }
}
