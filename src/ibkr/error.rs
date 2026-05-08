use thiserror::Error;

/// IBKR-specific error types
#[derive(Error, Debug, Clone)]
pub enum IbkrError {
    #[error("not connected to IBKR")]
    NotConnected,
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("market data subscription required (code {code}): {message}")]
    MarketDataSubscriptionRequired { code: i32, message: String },
    #[error("market data unavailable: {0}")]
    MarketDataUnavailable(String),
    #[error("invalid symbol: {0}")]
    InvalidSymbol(String),
    #[error("order placement failed: {0}")]
    OrderPlacementFailed(String),
    #[error("read-only mode")]
    ReadOnly,
    #[error("request timeout")]
    RequestTimeout,
    #[error("insufficient data")]
    InsufficientData,
    #[error("unknown error: {0}")]
    Unknown(String),
}

/// Check if an IBKR error code indicates a market data entitlement issue
pub fn is_entitlement_error(code: i32) -> bool {
    matches!(code, 10089 | 10167 | 10168 | 10169 | 354)
}

/// Map common IBKR error codes to domain errors
pub fn map_error_code(code: i32, message: String) -> IbkrError {
    if is_entitlement_error(code) {
        IbkrError::MarketDataSubscriptionRequired { code, message }
    } else {
        match code {
            200 => IbkrError::InvalidSymbol(message),
            502 => IbkrError::ConnectionFailed(message),
            399 => IbkrError::OrderPlacementFailed(message),
            420 => IbkrError::OrderPlacementFailed(message),
            _ => IbkrError::Unknown(format!("[{}] {}", code, message)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_entitlement_error_10089() {
        assert!(is_entitlement_error(10089));
    }

    #[test]
    fn test_is_entitlement_error_10167() {
        assert!(is_entitlement_error(10167));
    }

    #[test]
    fn test_is_entitlement_error_200() {
        assert!(!is_entitlement_error(200));
    }

    #[test]
    fn test_map_error_code_200() {
        let result = map_error_code(200, "msg".to_string());
        assert!(matches!(result, IbkrError::InvalidSymbol(ref msg) if msg == "msg"));
    }

    #[test]
    fn test_map_error_code_10089() {
        let result = map_error_code(10089, "msg".to_string());
        assert!(matches!(
            result,
            IbkrError::MarketDataSubscriptionRequired { code, ref message }
            if code == 10089 && message == "msg"
        ));
    }
}
