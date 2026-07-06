use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use ibapi::prelude::*;
use ibapi::contracts::OptionComputation;
use ibapi::contracts::tick_types::TickType;
use ibapi::market_data::MarketDataType;
use ibapi::subscriptions::SubscriptionItemStreamExt;
use futures::StreamExt;

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
    /// Option greeks and model fields (populated only for option contracts).
    pub delta: Option<f64>,
    pub gamma: Option<f64>,
    pub theta: Option<f64>,
    pub vega: Option<f64>,
    pub rho: Option<f64>,
    pub implied_volatility: Option<f64>,
    pub underlying_price: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QuoteSource {
    RealTime,
    Delayed,
    Frozen,
    DelayedFrozen,
    Cache,
}

/// Market data manager with caching and automatic fallback to frozen/delayed data
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
    /// with automatic cascade: Realtime → Frozen → DelayedFrozen → Delayed.
    pub async fn get_quote(&self,
        symbol: &str,
    ) -> Result<CachedQuote, IbkrError> {
        let symbol = symbol.to_uppercase();

        // Check cache
        if let Some(entry) = self.cache.get(&symbol) {
            let ttl = match entry.source {
                QuoteSource::RealTime => Duration::from_secs(self.config.real_time_ttl_secs),
                QuoteSource::Frozen => Duration::from_secs(self.config.frozen_ttl_secs),
                _ => Duration::from_secs(self.config.delayed_ttl_secs),
            };
            if entry.timestamp.elapsed() < ttl {
                info!(symbol = %symbol, source = ?entry.source, "Cache hit");
                return Ok(entry.clone());
            }
        }

        let is_delayed_symbol = self.delayed_symbols.contains_key(&symbol);

        // Build the cascade. If we already know this symbol requires delayed data,
        // skip Realtime and Frozen to avoid entitlement errors.
        let mut types_to_try = Vec::new();
        if !is_delayed_symbol {
            types_to_try.push(MarketDataType::Realtime);
            types_to_try.push(MarketDataType::Frozen);
        }
        types_to_try.push(MarketDataType::DelayedFrozen);
        types_to_try.push(MarketDataType::Delayed);

        for md_type in types_to_try {
            match self.fetch_quote(&symbol, md_type).await {
                Ok(quote) => {
                    self.cache.insert(symbol.clone(), quote.clone());
                    return Ok(quote);
                }
                Err(IbkrError::MarketDataSubscriptionRequired { code, .. }) => {
                    if md_type == MarketDataType::Realtime || md_type == MarketDataType::Frozen {
                        warn!(symbol = %symbol, code = code, "Entitlement error on realtime/frozen, will use delayed data going forward");
                        self.delayed_symbols.insert(symbol.clone(), ());
                    }
                    // Continue to next type in cascade
                }
                Err(IbkrError::MarketDataUnavailable(_)) => {
                    // No data for this type (e.g. market closed + no frozen data),
                    // continue to next type in cascade.
                }
                Err(e) => return Err(e),
            }
        }

        Err(IbkrError::MarketDataUnavailable(
            format!("No market data received for {} after trying all market data types", symbol),
        ))
    }

    /// Fetch quote via IBKR market data snapshot subscription for a specific MarketDataType.
    async fn fetch_quote(
        &self,
        symbol: &str,
        md_type: MarketDataType,
    ) -> Result<CachedQuote, IbkrError> {
        let client = self.client.get_client().await?;
        let contract = Contract::stock(symbol).build();

        info!(symbol = %symbol, ?md_type, "Fetching market data snapshot");

        // Switch to requested market data type (skip for Realtime — already default)
        if md_type != MarketDataType::Realtime {
            if let Err(e) = client.switch_market_data_type(md_type).await {
                warn!(symbol = %symbol, error = %e, "Failed to switch market data type");
            }
        }

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
            source: market_data_type_to_source(md_type),
            delta: None,
            gamma: None,
            theta: None,
            vega: None,
            rho: None,
            implied_volatility: None,
            underlying_price: None,
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
                    quote.source = market_data_type_to_source(dt);
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
        }

        // Switch back to real-time after non-realtime request
        if md_type != MarketDataType::Realtime {
            if let Err(e) = client.switch_market_data_type(MarketDataType::Realtime).await {
                warn!(symbol = %symbol, error = %e, "Failed to switch back to real-time market data type");
            }
        }

        if !got_data {
            return Err(IbkrError::MarketDataUnavailable(
                format!("No market data received for {} with {:?}", symbol, md_type),
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

    /// Get quotes for multiple symbols concurrently.
    /// Uses tokio::spawn per symbol — 12 symbols should return in ~15s instead of 12×15s.
    pub async fn get_bulk_quotes(
        market_data: Arc<MarketDataManager>,
        symbols: &[String],
    ) -> Vec<(String, Result<CachedQuote, IbkrError>)> {
        let tasks: Vec<_> = symbols
            .iter()
            .map(|symbol| {
                let md = Arc::clone(&market_data);
                let sym = symbol.clone();
                tokio::spawn(async move {
                    let result = md.get_quote(&sym).await;
                    (sym, result)
                })
            })
            .collect();

        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(pair) => results.push(pair),
                Err(e) => results.push((
                    String::new(),
                    Err(IbkrError::Unknown(format!("Task join error: {}", e))),
                )),
            }
        }
        results
    }

    /// Get option chain for a symbol
    pub async fn get_option_chain(
        &self,
        symbol: &str,
    ) -> Result<ibapi::contracts::OptionChain, IbkrError> {
        let client = self.client.get_client().await?;

        info!(symbol = %symbol, "Fetching option chain via sec_def_opt_params");

        let subscription = client
            .option_chain(symbol, "SMART", SecurityType::Stock, 0)
            .await
            .map_err(|e| IbkrError::Unknown(format!("option_chain request failed: {e}")))?;

        let mut result: Option<ibapi::contracts::OptionChain> = None;

        let mut data_stream = subscription.filter_data();

        while let Some(chain_result) = data_stream.next().await {
            match chain_result {
                Ok(chain) => {
                    result = Some(chain);
                }
                Err(e) => {
                    return Err(IbkrError::Unknown(format!(
                        "option_chain stream error: {e:?}"
                    )));
                }
            }
        }

        match result {
            Some(chain) => {
                info!(
                    symbol = %symbol,
                    expirations = chain.expirations.len(),
                    strikes = chain.strikes.len(),
                    "Option chain received"
                );
                Ok(chain)
            }
            None => Err(IbkrError::Unknown(format!(
                "No option chain data received for {symbol}"
            ))),
        }
    }

    /// Fetch a market data snapshot for an option contract.
    /// Uses the same Realtime → Frozen → DelayedFrozen → Delayed cascade as stock quotes.
    pub async fn get_option_quote(
        &self,
        contract: &ibapi::contracts::Contract,
        label: &str,
    ) -> Result<CachedQuote, IbkrError> {
        let cache_key = label.to_uppercase();

        // Check cache
        if let Some(entry) = self.cache.get(&cache_key) {
            let ttl = match entry.source {
                QuoteSource::RealTime => Duration::from_secs(self.config.real_time_ttl_secs),
                QuoteSource::Frozen => Duration::from_secs(self.config.frozen_ttl_secs),
                _ => Duration::from_secs(self.config.delayed_ttl_secs),
            };
            if entry.timestamp.elapsed() < ttl {
                info!(cache_key = %cache_key, source = ?entry.source, "Cache hit");
                return Ok(entry.clone());
            }
        }

        let is_delayed_symbol = self.delayed_symbols.contains_key(&cache_key);

        let mut types_to_try = Vec::new();
        if !is_delayed_symbol {
            types_to_try.push(MarketDataType::Realtime);
            types_to_try.push(MarketDataType::Frozen);
        }
        types_to_try.push(MarketDataType::DelayedFrozen);
        types_to_try.push(MarketDataType::Delayed);

        for md_type in types_to_try {
            match self.fetch_option_quote(contract, &cache_key, md_type).await {
                Ok(quote) => {
                    self.cache.insert(cache_key.clone(), quote.clone());
                    return Ok(quote);
                }
                Err(IbkrError::MarketDataSubscriptionRequired { code, .. }) => {
                    if md_type == MarketDataType::Realtime || md_type == MarketDataType::Frozen {
                        warn!(label = %cache_key, code = code, "Entitlement error on realtime/frozen, will use delayed data going forward");
                        self.delayed_symbols.insert(cache_key.clone(), ());
                    }
                }
                Err(IbkrError::MarketDataUnavailable(_)) => {
                    // Continue cascade
                }
                Err(e) => return Err(e),
            }
        }

        Err(IbkrError::MarketDataUnavailable(
            format!("No market data received for {} after trying all market data types", cache_key),
        ))
    }

    /// Internal: snapshot market data for an arbitrary contract (options, etc.)
    async fn fetch_option_quote(
        &self,
        contract: &ibapi::contracts::Contract,
        label: &str,
        md_type: MarketDataType,
    ) -> Result<CachedQuote, IbkrError> {
        let client = self.client.get_client().await?;

        info!(label = %label, ?md_type, "Fetching option market data snapshot");

        if md_type != MarketDataType::Realtime {
            if let Err(e) = client.switch_market_data_type(md_type).await {
                warn!(label = %label, error = %e, "Failed to switch market data type");
            }
        }

        // NOTE: Do NOT add generic_tick::OPTION_IMPLIED_VOLATILITY here.
        // IBKR error 321: "Snapshot market data subscription is not applicable
        // to generic ticks." Option computation ticks (model greeks) arrive
        // automatically for option contracts in snapshot mode.
        let mut subscription = client
            .market_data(contract)
            .snapshot()
            .subscribe()
            .await
            .map_err(|e| IbkrError::MarketDataUnavailable(e.to_string()))?;

        let mut quote = CachedQuote {
            symbol: label.to_string(),
            bid: 0.0,
            ask: 0.0,
            last: 0.0,
            volume: 0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            timestamp: Instant::now(),
            source: market_data_type_to_source(md_type),
            delta: None,
            gamma: None,
            theta: None,
            vega: None,
            rho: None,
            implied_volatility: None,
            underlying_price: None,
        };

        let mut got_data = false;

        while let Some(result) = subscription.next().await {
            match result {
                Ok(SubscriptionItem::Data(TickTypes::Price(p))) => {
                    match p.tick_type {
                        TickType::Bid | TickType::DelayedBid => { quote.bid = p.price; got_data = true; }
                        TickType::Ask | TickType::DelayedAsk => { quote.ask = p.price; got_data = true; }
                        TickType::Last | TickType::DelayedLast => { quote.last = p.price; got_data = true; }
                        TickType::High | TickType::DelayedHigh => { quote.high = p.price; }
                        TickType::Low | TickType::DelayedLow => { quote.low = p.price; }
                        TickType::Close | TickType::DelayedClose => { quote.close = p.price; }
                        _ => {}
                    }
                }
                Ok(SubscriptionItem::Data(TickTypes::Size(s))) => {
                    match s.tick_type {
                        TickType::BidSize | TickType::AskSize | TickType::LastSize
                        | TickType::DelayedBidSize | TickType::DelayedAskSize
                        | TickType::DelayedLastSize => { quote.volume += s.size as i64; }
                        _ => {}
                    }
                }
                Ok(SubscriptionItem::Data(TickTypes::PriceSize(ps))) => {
                    match ps.price_tick_type {
                        TickType::Bid | TickType::DelayedBid => { quote.bid = ps.price; got_data = true; }
                        TickType::Ask | TickType::DelayedAsk => { quote.ask = ps.price; got_data = true; }
                        TickType::Last | TickType::DelayedLast => { quote.last = ps.price; got_data = true; }
                        TickType::High | TickType::DelayedHigh => { quote.high = ps.price; }
                        TickType::Low | TickType::DelayedLow => { quote.low = ps.price; }
                        TickType::Close | TickType::DelayedClose => { quote.close = ps.price; }
                        _ => {}
                    }
                    match ps.size_tick_type {
                        TickType::BidSize | TickType::AskSize | TickType::LastSize
                        | TickType::DelayedBidSize | TickType::DelayedAskSize
                        | TickType::DelayedLastSize => { quote.volume += ps.size as i64; }
                        _ => {}
                    }
                }
                Ok(SubscriptionItem::Data(TickTypes::OptionComputation(c))) => {
                    apply_option_computation(&mut quote, &c);
                    // If we got model greeks, treat that as useful data even if no price ticks arrived yet.
                    if matches!(c.field, TickType::ModelOption | TickType::DelayedModelOption)
                        && c.delta.is_some()
                    {
                        got_data = true;
                    }
                }
                Ok(SubscriptionItem::Data(TickTypes::SnapshotEnd)) => {
                    info!(label = %label, "Snapshot complete");
                    break;
                }
                Ok(SubscriptionItem::Data(TickTypes::MarketDataType(dt))) => {
                    quote.source = market_data_type_to_source(dt);
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
        }

        if md_type != MarketDataType::Realtime {
            if let Err(e) = client.switch_market_data_type(MarketDataType::Realtime).await {
                warn!(label = %label, error = %e, "Failed to switch back to real-time");
            }
        }

        if !got_data {
            return Err(IbkrError::MarketDataUnavailable(
                format!("No market data received for {} with {:?}", label, md_type),
            ));
        }

        quote.timestamp = Instant::now();
        Ok(quote)
    }
}

/// Convert IBKR MarketDataType to our QuoteSource enum
fn market_data_type_to_source(md_type: MarketDataType) -> QuoteSource {
    match md_type {
        MarketDataType::Realtime => QuoteSource::RealTime,
        MarketDataType::Frozen => QuoteSource::Frozen,
        MarketDataType::Delayed => QuoteSource::Delayed,
        MarketDataType::DelayedFrozen => QuoteSource::DelayedFrozen,
        _ => QuoteSource::RealTime,
    }
}

/// Merge an IBKR option computation tick into a cached quote.
///
/// TWS sends multiple option computation ticks (bid/ask/last/model).  We
/// prefer model-based greeks (field == ModelOption / DelayedModelOption) and
/// only fill in missing values from the other computation types.  Note that
/// rust-ibapi's OptionComputation does not include rho, so rho remains None.
fn apply_option_computation(quote: &mut CachedQuote, computation: &OptionComputation) {
    let is_model = matches!(
        computation.field,
        TickType::ModelOption | TickType::DelayedModelOption
    );

    let merge = |current: &mut Option<f64>, incoming: Option<f64>, force: bool| {
        if let Some(value) = incoming {
            if current.is_none() || force {
                *current = Some(value);
            }
        }
    };

    merge(&mut quote.delta, computation.delta, is_model);
    merge(&mut quote.gamma, computation.gamma, is_model);
    merge(&mut quote.theta, computation.theta, is_model);
    merge(&mut quote.vega, computation.vega, is_model);
    merge(&mut quote.implied_volatility, computation.implied_volatility, is_model);
    merge(&mut quote.underlying_price, computation.underlying_price, is_model);

    tracing::debug!(
        field = ?computation.field,
        delta = ?computation.delta,
        gamma = ?computation.gamma,
        theta = ?computation.theta,
        vega = ?computation.vega,
        iv = ?computation.implied_volatility,
        "Option computation tick"
    );
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
