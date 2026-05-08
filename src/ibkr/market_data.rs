use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use ibapi::prelude::*;
use ibapi::market_data::realtime::TickType;
use ibapi::market_data::MarketDataType;

use crate::config::MarketDataConfig;
use crate::ibkr::client::IbkrClient;
use crate::ibkr::error::{IbkrError, is_entitlement_error};

/// Try to extract an IB error code from an error string like "[10089] ..."
fn extract_error_code(s: &str) -> Option<i32> {
    s.strip_prefix('[')?
        .split(']')
        .next()?
        .parse()
        .ok()
}
/// A cached market data quote
#[derive(Debug, Clone)]
pub struct CachedQuote {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub volume: i64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub timestamp: Instant,
    pub source: QuoteSource,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QuoteSource {
    RealTime,
    Delayed,
    Cache,
}

/// Market data manager with caching and delayed fallback
#[derive(Debug)]
pub struct MarketDataManager {
    client: Arc<IbkrClient>,
    config: MarketDataConfig,
    cache: DashMap<String, CachedQuote>,
    delayed_symbols: DashMap<String, ()>,
}

impl MarketDataManager {
    pub fn new(client: Arc<IbkrClient>, config: MarketDataConfig) -> Self {
        Self {
            client,
            config: config.clone(),
            cache: DashMap::with_capacity(config.max_cache_entries),
            delayed_symbols: DashMap::new(),
        }
    }

    /// Get a quote for a symbol. Checks cache first, then fetches from IBKR
    /// with automatic delayed fallback on entitlement errors.
    pub async fn get_quote(&self,
        symbol: &str,
    ) -> Result<CachedQuote, IbkrError> {
        let symbol = symbol.to_uppercase();

        // Check cache
        if let Some(entry) = self.cache.get(&symbol) {
            let ttl = match entry.source {
                QuoteSource::RealTime => Duration::from_secs(self.config.real_time_ttl_secs),
                _ => Duration::from_secs(self.config.delayed_ttl_secs),
            };
            if entry.timestamp.elapsed() < ttl {
                info!(symbol = %symbol, source = ?entry.source, "Cache hit");
                return Ok(entry.clone());
            }
        }

        // Try real-time first, delayed if marked or on entitlement error
        let use_delayed = self.delayed_symbols.contains_key(&symbol);

        match self.fetch_quote(&symbol, use_delayed).await {
            Ok(quote) => {
                self.cache.insert(symbol.clone(), quote.clone());
                Ok(quote)
            }
            Err(IbkrError::MarketDataSubscriptionRequired { code, .. }) => {
                warn!(symbol = %symbol, code = code, "Entitlement error, retrying with delayed data");
                self.delayed_symbols.insert(symbol.clone(), ());

                let quote = self.fetch_quote(&symbol, true).await?;
                self.cache.insert(symbol.clone(), quote.clone());
                Ok(quote)
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch quote via IBKR market data snapshot subscription
    async fn fetch_quote(
        &self,
        symbol: &str,
        use_delayed: bool,
    ) -> Result<CachedQuote, IbkrError> {
        let client = self.client.get_client().await?;
        let contract = Contract::stock(symbol).build();

        info!(symbol = %symbol, delayed = use_delayed, "Fetching market data snapshot");

        // Switch to delayed market data type if requested
        if use_delayed {
            if let Err(e) = client.switch_market_data_type(ibapi::market_data::MarketDataType::Delayed).await {
                warn!(symbol = %symbol, error = %e, "Failed to switch to delayed market data type");
            }
        }

        // In ibapi v3, market data is always subscription-based.
        // Use `.snapshot()` for a one-time request that auto-cancels after first tick.
        let mut subscription = client
            .market_data(&contract)
            .snapshot()
            .subscribe()
            .await
            .map_err(|e| IbkrError::MarketDataUnavailable(e.to_string()))?;

        let mut quote = CachedQuote {
            symbol: symbol.to_string(),
            bid: 0.0,
            ask: 0.0,
            last: 0.0,
            volume: 0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            timestamp: Instant::now(),
            source: if use_delayed { QuoteSource::Delayed } else { QuoteSource::RealTime },
        };

        let mut got_data = false;

        while let Some(result) = subscription.next().await {
            match result {
                Ok(SubscriptionItem::Data(TickTypes::Price(p))) => {
                    match p.tick_type {
                        TickType::Bid | TickType::DelayedBid => {
                            quote.bid = p.price;
                            got_data = true;
                        }
                        TickType::Ask | TickType::DelayedAsk => {
                            quote.ask = p.price;
                            got_data = true;
                        }
                        TickType::Last | TickType::DelayedLast => {
                            quote.last = p.price;
                            got_data = true;
                        }
                        TickType::High | TickType::DelayedHigh => {
                            quote.high = p.price;
                        }
                        TickType::Low | TickType::DelayedLow => {
                            quote.low = p.price;
                        }
                        TickType::Close | TickType::DelayedClose => {
                            quote.close = p.price;
                        }
                        _ => {}
                    }
                }
                Ok(SubscriptionItem::Data(TickTypes::Size(s))) => {
                    match s.tick_type {
                        TickType::BidSize | TickType::AskSize | TickType::LastSize
                        | TickType::DelayedBidSize | TickType::DelayedAskSize
                        | TickType::DelayedLastSize => {
                            quote.volume += s.size as i64;
                        }
                        _ => {}
                    }
                }
                Ok(SubscriptionItem::Data(TickTypes::PriceSize(ps))) => {
                    match ps.price_tick_type {
                        TickType::Bid | TickType::DelayedBid => {
                            quote.bid = ps.price;
                            got_data = true;
                        }
                        TickType::Ask | TickType::DelayedAsk => {
                            quote.ask = ps.price;
                            got_data = true;
                        }
                        TickType::Last | TickType::DelayedLast => {
                            quote.last = ps.price;
                            got_data = true;
                        }
                        TickType::High | TickType::DelayedHigh => {
                            quote.high = ps.price;
                        }
                        TickType::Low | TickType::DelayedLow => {
                            quote.low = ps.price;
                        }
                        TickType::Close | TickType::DelayedClose => {
                            quote.close = ps.price;
                        }
                        _ => {}
                    }
                    match ps.size_tick_type {
                        TickType::BidSize | TickType::AskSize | TickType::LastSize
                        | TickType::DelayedBidSize | TickType::DelayedAskSize
                        | TickType::DelayedLastSize => {
                            quote.volume += ps.size as i64;
                        }
                        _ => {}
                    }
                }
                Ok(SubscriptionItem::Data(TickTypes::SnapshotEnd)) => {
                    info!(symbol = %symbol, "Snapshot complete");
                    break;
                }
                Ok(SubscriptionItem::Data(TickTypes::MarketDataType(dt))) => {
                    // Track whether we got delayed or real-time data
                    quote.source = match dt {
                        MarketDataType::Delayed => QuoteSource::Delayed,
                        MarketDataType::DelayedFrozen => QuoteSource::Delayed,
                        _ => quote.source,
                    };
                }
                Ok(SubscriptionItem::Notice(notice)) => {
                    if is_entitlement_error(notice.code) {
                        return Err(IbkrError::MarketDataSubscriptionRequired {
                            code: notice.code,
                            message: notice.message.clone(),
                        });
                    }
                    warn!(code = notice.code, message = %notice.message, "Market data notice");
                }
                Err(e) => {
                    let err_str = e.to_string();
                    // Entitlement errors come as Err, not as Notice
                    if let Some(code) = extract_error_code(&err_str) {
                        if is_entitlement_error(code) {
                            return Err(IbkrError::MarketDataSubscriptionRequired {
                                code,
                                message: err_str,
                            });
                        }
                    }
                    return Err(IbkrError::MarketDataUnavailable(err_str));
                }
                _ => {}
            }

            if got_data {
                // We got at least a price tick, can return early
                // or wait for SnapshotEnd for completeness
            }
        }

        // Switch back to real-time after delayed request
        if use_delayed {
            if let Err(e) = client.switch_market_data_type(ibapi::market_data::MarketDataType::Realtime).await {
                warn!(symbol = %symbol, error = %e, "Failed to switch back to real-time market data type");
            }
        }

        if !got_data {
            return Err(IbkrError::MarketDataUnavailable(
                "No market data received".to_string(),
            ));
        }

        quote.timestamp = Instant::now();
        Ok(quote)
    }

    /// Get historical data for a symbol
    pub async fn get_historical(
        &self,
        symbol: &str,
        _period: &str,
    ) -> Result<Vec<HistoricalBar>, IbkrError> {
        let _client = self.client.get_client().await?;
        let _contract = Contract::stock(symbol).build();

        info!(symbol = %symbol, "Fetching historical data");

        // TODO: Implement using ibapi v3's historical_data API
        Err(IbkrError::Unknown(
            "Historical data not yet implemented".to_string(),
        ))
    }

    /// Get option chain for a symbol
    pub async fn get_option_chain(
        &self,
        symbol: &str,
    ) -> Result<OptionChain, IbkrError> {
        let _client = self.client.get_client().await?;

        info!(symbol = %symbol, "Fetching option chain");

        // TODO: Implement using ibapi v3's sec_def_opt_params
        Err(IbkrError::Unknown(
            "Option chain not yet implemented".to_string(),
        ))
    }
}

/// Historical bar data
#[derive(Debug, Clone)]
pub struct HistoricalBar {
    pub timestamp: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
}

/// Option chain data
#[derive(Debug, Clone)]
pub struct OptionChain {
    pub symbol: String,
    pub expirations: Vec<String>,
    pub strikes: Vec<f64>,
}
