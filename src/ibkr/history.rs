//! Historical bars, IV rank/percentile, and historical trade ticks.
//!
//! Read-only data paths: no order/execution capability lives here. Every IBKR
//! request is wrapped in a `tokio::time::timeout` and every subscription is
//! explicitly cancelled, so a stalled gateway can never pin an MCP session open
//! or leak a request slot.
//!
//! The 2026-09-16 MarketChameleon cross-check established the numbers this
//! module must reproduce: WOLF 1-year daily `OPTION_IMPLIED_VOLATILITY` →
//! ~234 bars, IV rank 19.2, percentile 12.8. [`compute_iv_rank`] is that exact
//! formula, extracted as a pure function so it can be unit-tested without a
//! broker.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use futures::StreamExt;
use ibapi::contracts::Contract;
use ibapi::market_data::historical::{
    Bar, BarSize, BarTimestamp, Duration as IbkrDuration, HistoricalData, TickBidAsk, TickDecoder,
    TickLast, TickMidpoint, TickSubscription, WhatToShow,
};
use ibapi::prelude::*;
use time::format_description::well_known::Rfc3339;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime};
use tracing::{info, warn};

use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::{is_entitlement_error, IbkrError};

/// Wall-clock ceiling for a batch of historical bars (IBKR answers in 1–5 s).
const BARS_TIMEOUT_SECS: u64 = 60;
/// Idle ceiling between historical-tick batches.
const TICKS_IDLE_SECS: u64 = 5;
/// IBKR caps historical ticks at 1000 per request.
pub const MAX_TICKS: i32 = 1000;

// ============================================================================
// Bars
// ============================================================================

/// A fully-specified historical bar request.
#[derive(Debug, Clone, Copy)]
pub struct BarRequest {
    pub duration: IbkrDuration,
    pub bar_size: BarSize,
    pub what_to_show: WhatToShow,
    pub use_rth: bool,
    pub end: Option<OffsetDateTime>,
}

/// One historical bar, normalised for JSON output.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BarRecord {
    /// ISO-8601 date for daily+ bars, RFC3339 timestamp for intraday bars.
    pub date: String,
    /// Unix seconds for intraday bars; null for daily+ bars.
    pub timestamp_unix: Option<i64>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Traded volume — populated by IBKR for `TRADES` only.
    pub volume: f64,
    /// Weighted average price — populated by IBKR for `TRADES` only.
    pub wap: f64,
    /// Trade count — populated by IBKR for `TRADES` only.
    pub count: i32,
}

/// Historical bars for one contract, oldest → newest.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HistoricalSeries {
    pub bars: Vec<BarRecord>,
    pub first_date: Option<String>,
    pub last_date: Option<String>,
}

impl HistoricalSeries {
    fn new(bars: Vec<BarRecord>) -> Self {
        let first_date = bars.first().map(|b| b.date.clone());
        let last_date = bars.last().map(|b| b.date.clone());
        Self {
            bars,
            first_date,
            last_date,
        }
    }
}

/// Fetch historical bars for an already-built contract.
///
/// Shared by [`HistoricalDataManager::get_historical_data`] and
/// [`crate::ibkr::market_data::MarketDataManager::get_historical`] so the
/// `data_type='historical'` path has exactly one implementation.
pub async fn fetch_bars(
    client: &Client,
    contract: &Contract,
    req: &BarRequest,
) -> Result<HistoricalSeries, IbkrError> {
    let mut builder = client
        .historical_data(contract, req.bar_size)
        .what_to_show(req.what_to_show)
        .trading_hours(trading_hours(req.use_rth))
        .duration(req.duration);

    if let Some(end) = req.end {
        builder = builder.ending(end);
    }

    let data: HistoricalData =
        tokio::time::timeout(StdDuration::from_secs(BARS_TIMEOUT_SECS), builder.fetch())
            .await
            .map_err(|_| IbkrError::RequestTimeout)?
            .map_err(map_ibapi_error)?;

    Ok(to_series(data))
}

fn to_series(data: HistoricalData) -> HistoricalSeries {
    let mut bars: Vec<Bar> = data.bars;
    // IBKR sends bars oldest → newest; sort anyway so "last bar" is a
    // property of the data, not of the wire order.
    bars.sort_by(|a, b| a.date.cmp(&b.date));
    HistoricalSeries::new(bars.into_iter().map(bar_record).collect())
}

fn bar_record(bar: Bar) -> BarRecord {
    let (date, timestamp_unix) = match bar.date {
        BarTimestamp::Date(d) => (d.to_string(), None),
        BarTimestamp::DateTime(dt) => (
            dt.format(&Rfc3339).unwrap_or_else(|_| dt.to_string()),
            Some(dt.unix_timestamp()),
        ),
    };

    BarRecord {
        date,
        timestamp_unix,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        wap: bar.wap,
        count: bar.count,
    }
}

// ============================================================================
// IV rank
// ============================================================================

/// IV summary over a trailing window of daily observations.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IvStats {
    /// Most recent observation — the "current" IV.
    pub iv: f64,
    pub iv_rank_pct: f64,
    pub iv_percentile: f64,
    pub iv_52w_low: f64,
    pub iv_52w_high: f64,
    /// Observations actually used.
    pub bars: usize,
    /// Observations requested.
    pub window_requested: usize,
}

/// IV rank / percentile over the most recent `window` positive observations.
///
/// `iv_rank_pct = (iv - low) / (high - low) * 100`;
/// `iv_percentile = 100 * count(v < iv) / n`.
///
/// Non-finite and non-positive values are dropped first — IBKR emits 0.0 for
/// bars it has no IV for, and including them would deflate the range.
pub fn compute_iv_rank(closes: &[f64], window: usize) -> Option<IvStats> {
    let vals: Vec<f64> = closes
        .iter()
        .copied()
        .filter(|v| v.is_finite() && *v > 0.0)
        .collect();

    if vals.is_empty() {
        return None;
    }

    let n = vals.len();
    let used = window.max(1).min(n);
    let slice = &vals[n - used..];

    let iv = *slice.last()?;
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    for v in slice {
        if *v < low {
            low = *v;
        }
        if *v > high {
            high = *v;
        }
    }

    let iv_rank_pct = if high > low {
        (iv - low) / (high - low) * 100.0
    } else {
        0.0
    };
    let below = slice.iter().filter(|v| **v < iv).count();
    let iv_percentile = 100.0 * below as f64 / used as f64;

    Some(IvStats {
        iv,
        iv_rank_pct,
        iv_percentile,
        iv_52w_low: low,
        iv_52w_high: high,
        bars: used,
        window_requested: window,
    })
}

// ============================================================================
// Historical ticks
// ============================================================================

/// Which historical-tick series to request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickKind {
    Trades,
    MidPoint,
    BidAsk,
}

impl TickKind {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "TRADES" | "TRADE" | "LAST" => Some(Self::Trades),
            "MIDPOINT" => Some(Self::MidPoint),
            "BID_ASK" | "BIDASK" => Some(Self::BidAsk),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Trades => "TRADES",
            Self::MidPoint => "MIDPOINT",
            Self::BidAsk => "BID_ASK",
        }
    }
}

/// A historical tick request.
#[derive(Debug, Clone)]
pub struct TicksRequest {
    pub symbol: String,
    pub number_of_ticks: i32,
    pub kind: TickKind,
    pub start: Option<OffsetDateTime>,
    pub end: Option<OffsetDateTime>,
    pub use_rth: bool,
}

/// One historical tick, normalised for JSON output.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TickRecord {
    pub timestamp: String,
    pub price: f64,
    pub size: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_bid: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_ask: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bid: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_ask: Option<f64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub exchange: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub conditions: String,
}

fn ts_string(dt: OffsetDateTime) -> String {
    dt.format(&Rfc3339).unwrap_or_else(|_| dt.to_string())
}

// ============================================================================
// Manager
// ============================================================================

/// Owns the historical-data surface. Stateless apart from the shared client.
#[derive(Debug)]
pub struct HistoricalDataManager {
    client: Arc<IbkrClient>,
}

impl HistoricalDataManager {
    pub fn new(client: Arc<IbkrClient>) -> Self {
        Self { client }
    }

    /// Fetch historical bars for a stock symbol.
    pub async fn get_historical_data(
        &self,
        symbol: &str,
        req: &BarRequest,
    ) -> Result<HistoricalSeries, IbkrError> {
        let client = self.client.get_client().await?;
        let contract = Contract::stock(symbol).build();

        info!(
            symbol = %symbol,
            what_to_show = %req.what_to_show,
            bar_size = %req.bar_size,
            duration = %req.duration,
            "Fetching historical bars"
        );

        fetch_bars(&client, &contract, req).await
    }

    /// Daily `OPTION_IMPLIED_VOLATILITY` series → IV rank / percentile.
    ///
    /// IBKR serves one-day bars for at most `1 Y`, so the fetch is fixed at
    /// `1 Y` and `window` is an upper bound on how many of the returned bars
    /// are used. `window_requested` vs `bars` in the result keeps that honest
    /// instead of silently pretending 252 bars were available.
    pub async fn get_iv_rank(
        &self,
        symbol: &str,
        window: usize,
    ) -> Result<(IvStats, String, String), IbkrError> {
        let req = BarRequest {
            duration: IbkrDuration::YEAR,
            bar_size: BarSize::Day,
            what_to_show: WhatToShow::OptionImpliedVolatility,
            use_rth: true,
            end: None,
        };

        let series = self.get_historical_data(symbol, &req).await?;

        if series.bars.is_empty() {
            return Err(IbkrError::InsufficientData);
        }

        let closes: Vec<f64> = series.bars.iter().map(|b| b.close).collect();
        let stats = compute_iv_rank(&closes, window).ok_or(IbkrError::InsufficientData)?;

        let first = series
            .bars
            .first()
            .map(|b| b.date.clone())
            .unwrap_or_default();
        let last = series
            .bars
            .last()
            .map(|b| b.date.clone())
            .unwrap_or_default();

        Ok((stats, first, last))
    }

    /// Historical trade / midpoint / bid-ask ticks (trade prints).
    pub async fn get_historical_ticks(
        &self,
        req: &TicksRequest,
    ) -> Result<Vec<TickRecord>, IbkrError> {
        let client = self.client.get_client().await?;
        let contract = Contract::stock(&req.symbol).build();

        info!(
            symbol = %req.symbol,
            kind = req.kind.as_str(),
            ticks = req.number_of_ticks,
            "Fetching historical ticks"
        );

        let ticks = req.number_of_ticks.clamp(1, MAX_TICKS);

        let mut builder = client
            .historical_ticks(&contract, ticks)
            .trading_hours(trading_hours(req.use_rth));
        if let Some(start) = req.start {
            builder = builder.starting(start);
        }
        if let Some(end) = req.end {
            builder = builder.ending(end);
        }

        match req.kind {
            TickKind::Trades => {
                let sub = builder.trade().await.map_err(map_ibapi_error)?;
                drain_ticks(sub, |t: TickLast| TickRecord {
                    timestamp: ts_string(t.timestamp),
                    price: t.price,
                    size: t.size,
                    price_bid: None,
                    price_ask: None,
                    size_bid: None,
                    size_ask: None,
                    exchange: t.exchange,
                    conditions: t.special_conditions,
                })
                .await
            }
            TickKind::MidPoint => {
                let sub = builder.mid_point().await.map_err(map_ibapi_error)?;
                drain_ticks(sub, |t: TickMidpoint| TickRecord {
                    timestamp: ts_string(t.timestamp),
                    price: t.price,
                    size: t.size,
                    price_bid: None,
                    price_ask: None,
                    size_bid: None,
                    size_ask: None,
                    exchange: String::new(),
                    conditions: String::new(),
                })
                .await
            }
            TickKind::BidAsk => {
                let sub = builder
                    .bid_ask(IgnoreSize::No)
                    .await
                    .map_err(map_ibapi_error)?;
                drain_ticks(sub, |t: TickBidAsk| TickRecord {
                    timestamp: ts_string(t.timestamp),
                    price: (t.price_bid + t.price_ask) / 2.0,
                    size: None,
                    price_bid: Some(t.price_bid),
                    price_ask: Some(t.price_ask),
                    size_bid: t.size_bid,
                    size_ask: t.size_ask,
                    exchange: String::new(),
                    conditions: String::new(),
                })
                .await
            }
        }
    }
}

/// Drain a tick subscription until it ends or goes idle, then cancel it.
///
/// `TickSubscription` is a `Stream<Item = Result<SubscriptionItem<T>, Error>>`;
/// `None` means the batch stream is finished. The idle timeout matters because
/// a request that IBKR never answers would otherwise hang the MCP session.
async fn drain_ticks<T, F>(
    mut sub: TickSubscription<T>,
    map: F,
) -> Result<Vec<TickRecord>, IbkrError>
where
    T: TickDecoder<T> + Send + Unpin,
    F: Fn(T) -> TickRecord,
{
    let mut out = Vec::new();

    loop {
        match tokio::time::timeout(StdDuration::from_secs(TICKS_IDLE_SECS), sub.next()).await {
            Ok(Some(Ok(SubscriptionItem::Data(tick)))) => out.push(map(tick)),
            Ok(Some(Ok(SubscriptionItem::Notice(n)))) => {
                if is_entitlement_error(n.code) {
                    sub.cancel().await;
                    return Err(IbkrError::MarketDataSubscriptionRequired {
                        code: n.code,
                        message: n.message,
                    });
                }
                warn!(code = n.code, message = %n.message, "Historical ticks notice");
            }
            Ok(Some(Err(e))) => {
                sub.cancel().await;
                return Err(map_ibapi_error(e));
            }
            Ok(None) => break,
            // Idle: IBKR has nothing more for this request.
            Err(_) => break,
        }
    }

    sub.cancel().await;
    Ok(out)
}

// ============================================================================
// Parsing / error helpers
// ============================================================================

/// Parse an IBKR bar-size string.
///
/// Accepts both the IBKR wire spelling (`"1 day"`, `"15 mins"`, `"5 secs"`) and
/// the shorthand `ibapi::BarSize` spelling (`"DAY"`, `"MIN15"`, `"SEC5"`), so a
/// caller can paste either form. `BarSize::from_str` only accepts the shorthand
/// form, which is why this normaliser exists.
pub fn parse_bar_size(s: &str) -> Result<BarSize, String> {
    let raw = s.trim();
    if raw.is_empty() {
        return Err("bar_size is empty — expected e.g. '1 day', '15 mins', '1 hour'".to_string());
    }

    let upper = raw.to_uppercase();
    let (count, unit_raw) = match upper.find(|c: char| !c.is_ascii_digit()) {
        Some(idx) if idx > 0 => {
            let count: i32 = upper[..idx]
                .parse()
                .map_err(|e| format!("invalid bar_size '{raw}': {e}"))?;
            (count, upper[idx..].trim().to_string())
        }
        Some(_) => (1, upper.clone()),
        None => (upper.parse::<i32>().unwrap_or(1), String::new()),
    };

    let unit_raw = unit_raw.trim_start_matches([' ', '_', '-']).to_string();
    let unit = match unit_raw.trim_end_matches('S') {
        "SEC" => "SEC",
        "MIN" => "MIN",
        "HOUR" => "HOUR",
        "DAY" => "DAY",
        "WEEK" => "WEEK",
        "MONTH" => "MONTH",
        "" => match upper.trim_end_matches('S') {
            "SEC" => "SEC",
            "MIN" => "MIN",
            "HOUR" => "HOUR",
            "DAY" => "DAY",
            "WEEK" => "WEEK",
            "MONTH" => "MONTH",
            other => other,
        },
        other => other,
    };

    let normalized = if count == 1 {
        unit.to_string()
    } else {
        format!("{unit}{count}")
    };

    BarSize::from_str(&normalized)
        .map_err(|_| format!("invalid bar_size '{raw}' — expected e.g. '1 min', '15 mins', '1 hour', '1 day'"))
}

/// Parse `YYYYMMDD` or `YYYY-MM-DD` into a date.
pub fn parse_date(s: &str) -> Result<Date, String> {
    let s = s.trim();
    let (y, m, d) = if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        (
            s[0..4].parse::<i32>(),
            s[4..6].parse::<u8>(),
            s[6..8].parse::<u8>(),
        )
    } else {
        let parts: Vec<&str> = s.split(['-', '/']).collect();
        if parts.len() != 3 {
            return Err(format!(
                "invalid date '{s}' — expected YYYYMMDD or YYYY-MM-DD"
            ));
        }
        (
            parts[0].parse::<i32>(),
            parts[1].parse::<u8>(),
            parts[2].parse::<u8>(),
        )
    };

    let year = y.map_err(|e| format!("invalid year in '{s}': {e}"))?;
    let month_u8 = m.map_err(|e| format!("invalid month in '{s}': {e}"))?;
    let day = d.map_err(|e| format!("invalid day in '{s}': {e}"))?;
    let month = Month::try_from(month_u8).map_err(|e| format!("invalid month in '{s}': {e}"))?;

    Date::from_calendar_date(year, month, day).map_err(|e| format!("invalid date '{s}': {e}"))
}

/// Parse an end date/datetime. Accepts `""` (now), `YYYYMMDD`, `YYYY-MM-DD`,
/// `YYYY-MM-DD HH:MM:SS` and RFC3339 (`2026-09-16T20:00:00Z`); datetimes are UTC.
pub fn parse_datetime(s: &str) -> Result<Option<OffsetDateTime>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }

    // RFC3339 first — unambiguous.
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(Some(dt));
    }

    let normalized = s.replace('T', " ").replace('Z', "");
    let (date_part, time_part) = match normalized.split_once(' ') {
        Some((d, t)) => (d, Some(t.trim())),
        None => (normalized.as_str(), None),
    };

    let date = parse_date(date_part)?;

    let Some(time_part) = time_part.filter(|t| !t.is_empty()) else {
        return Ok(Some(date.midnight().assume_utc()));
    };

    let parts: Vec<&str> = time_part.split(':').collect();
    if parts.len() < 2 {
        return Err(format!("invalid time '{time_part}' — expected HH:MM[:SS]"));
    }
    let hour: u8 = parts[0]
        .parse()
        .map_err(|e| format!("invalid hour '{time_part}': {e}"))?;
    let minute: u8 = parts[1]
        .parse()
        .map_err(|e| format!("invalid minute '{time_part}': {e}"))?;
    let second: u8 = if parts.len() > 2 {
        parts[2]
            .split('.')
            .next()
            .unwrap_or("0")
            .parse()
            .map_err(|e| format!("invalid second '{time_part}': {e}"))?
    } else {
        0
    };

    let dt = PrimitiveDateTime::new(
        date,
        time::Time::from_hms(hour, minute, second)
            .map_err(|e| format!("invalid time '{time_part}': {e}"))?,
    );

    Ok(Some(dt.assume_utc()))
}

/// Map an `ibapi::Error` into our domain error, preserving IBKR entitlement codes.
pub fn map_ibapi_error(err: ibapi::Error) -> IbkrError {
    let message = err.to_string();

    if let Some(code) = extract_ib_code(&message) {
        if is_entitlement_error(code) {
            return IbkrError::MarketDataSubscriptionRequired { code, message };
        }
    }

    match err {
        ibapi::Error::InvalidArgument(_) => IbkrError::Unknown(message),
        _ => IbkrError::MarketDataUnavailable(message),
    }
}

/// Extract the numeric code from an IBKR error string like `[10089] ...`.
pub fn extract_ib_code(s: &str) -> Option<i32> {
    let start = s.find('[')?;
    let rest = &s[start + 1..];
    let end = rest.find(']')?;
    rest[..end].trim().parse().ok()
}

pub(crate) fn trading_hours(use_rth: bool) -> TradingHours {
    if use_rth {
        TradingHours::Regular
    } else {
        TradingHours::Extended
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iv_rank_matches_probe_formula() {
        // 234 straight-line observations climbing 80 → 100, newest last.
        let closes: Vec<f64> = (0..234).map(|i| 80.0 + i as f64 * (20.0 / 233.0)).collect();
        let stats = compute_iv_rank(&closes, 252).unwrap();

        assert!((stats.iv_52w_low - 80.0).abs() < 1e-9);
        assert!((stats.iv_52w_high - 100.0).abs() < 1e-9);
        assert!((stats.iv - 100.0).abs() < 1e-9);
        assert!((stats.iv_rank_pct - 100.0).abs() < 1e-9);
        // Every earlier observation is strictly below the last one.
        assert!((stats.iv_percentile - 100.0 * 233.0 / 234.0).abs() < 1e-9);
        assert_eq!(stats.bars, 234);
    }

    #[test]
    fn iv_rank_drops_non_positive_observations() {
        let closes = vec![0.0, 50.0, -1.0, f64::NAN, 60.0, 40.0];
        let stats = compute_iv_rank(&closes, 252).unwrap();
        assert_eq!(stats.bars, 3);
        assert!((stats.iv_52w_low - 40.0).abs() < 1e-9);
        assert!((stats.iv_52w_high - 60.0).abs() < 1e-9);
        assert!((stats.iv - 40.0).abs() < 1e-9);
        assert!((stats.iv_rank_pct - 0.0).abs() < 1e-9);
        assert!((stats.iv_percentile - 0.0).abs() < 1e-9);
    }

    #[test]
    fn iv_rank_window_is_an_upper_bound() {
        let closes: Vec<f64> = (1..=10).map(|i| i as f64).collect();
        let stats = compute_iv_rank(&closes, 4).unwrap();
        assert_eq!(stats.bars, 4);
        assert!((stats.iv_52w_low - 7.0).abs() < 1e-9);
        assert!((stats.iv_52w_high - 10.0).abs() < 1e-9);
        assert!((stats.iv - 10.0).abs() < 1e-9);
        assert_eq!(stats.window_requested, 4);
    }

    #[test]
    fn iv_rank_flat_series_is_zero_not_nan() {
        let closes = vec![30.0; 5];
        let stats = compute_iv_rank(&closes, 252).unwrap();
        assert!((stats.iv_rank_pct - 0.0).abs() < 1e-9);
        assert!((stats.iv_percentile - 0.0).abs() < 1e-9);
    }

    #[test]
    fn iv_rank_empty_is_none() {
        assert!(compute_iv_rank(&[], 252).is_none());
        assert!(compute_iv_rank(&[0.0, -3.0], 252).is_none());
    }

    #[test]
    fn parse_date_accepts_both_forms() {
        assert_eq!(parse_date("2026-09-16").unwrap().to_string(), "2026-09-16");
        assert_eq!(parse_date("20260916").unwrap().to_string(), "2026-09-16");
        assert!(parse_date("2026-13-01").is_err());
        assert!(parse_date("nonsense").is_err());
    }

    #[test]
    fn parse_datetime_accepts_the_documented_forms() {
        assert!(parse_datetime("").unwrap().is_none());

        let rfc = parse_datetime("2026-09-16T20:00:00Z").unwrap().unwrap();
        let spaced = parse_datetime("2026-09-16 20:00:00").unwrap().unwrap();
        assert_eq!(rfc.unix_timestamp(), spaced.unix_timestamp());

        let date_only = parse_datetime("20260916").unwrap().unwrap();
        assert_eq!(date_only.unix_timestamp(), rfc.unix_timestamp() - 20 * 3600);

        assert!(parse_datetime("2026-09-16T25:00:00Z").is_err());
    }

    #[test]
    fn bar_size_accepts_ibkr_and_shorthand_spellings() {
        assert_eq!(parse_bar_size("1 day").unwrap(), BarSize::Day);
        assert_eq!(parse_bar_size("DAY").unwrap(), BarSize::Day);
        assert_eq!(parse_bar_size("1 min").unwrap(), BarSize::Min);
        assert_eq!(parse_bar_size("15 mins").unwrap(), BarSize::Min15);
        assert_eq!(parse_bar_size("MIN5").unwrap(), BarSize::Min5);
        assert_eq!(parse_bar_size("5 secs").unwrap(), BarSize::Sec5);
        assert_eq!(parse_bar_size("1 hour").unwrap(), BarSize::Hour);
        assert_eq!(parse_bar_size("4 hours").unwrap(), BarSize::Hour4);
        assert_eq!(parse_bar_size("1 week").unwrap(), BarSize::Week);
        assert_eq!(parse_bar_size("1 month").unwrap(), BarSize::Month);

        assert!(parse_bar_size("").is_err());
        assert!(parse_bar_size("banana").is_err());
        assert!(parse_bar_size("7 days").is_err());
        assert!(parse_bar_size("3 months").is_err());
    }

    #[test]
    fn tick_kind_parsing() {
        assert_eq!(TickKind::parse("trades"), Some(TickKind::Trades));
        assert_eq!(TickKind::parse("MIDPOINT"), Some(TickKind::MidPoint));
        assert_eq!(TickKind::parse("bid_ask"), Some(TickKind::BidAsk));
        assert_eq!(TickKind::parse("nope"), None);
    }

    #[test]
    fn ib_code_extraction() {
        assert_eq!(extract_ib_code("[10089] no subscription"), Some(10089));
        assert_eq!(extract_ib_code("error [200] no such symbol"), Some(200));
        assert_eq!(extract_ib_code("plain error"), None);
        assert_eq!(extract_ib_code("[abc] nope"), None);
    }
}
