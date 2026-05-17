use ibapi::prelude::*;

/// Build a stock contract
pub fn stock(symbol: &str) -> Contract {
    Contract::stock(symbol).build()
}

/// Build a call option contract
pub fn option_call(symbol: &str, strike: f64, year: i32, month: u32, day: u32) -> Contract {
    Contract::call(symbol)
        .strike(strike)
        .expires_on(year as u16, month as u8, day as u8)
        .build()
}

/// Build a put option contract
pub fn option_put(symbol: &str, strike: f64, year: i32, month: u32, day: u32) -> Contract {
    Contract::put(symbol)
        .strike(strike)
        .expires_on(year as u16, month as u8, day as u8)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stock_contract() {
        let contract = stock("AAPL");
        assert_eq!(contract.symbol, Symbol::from("AAPL"));
        assert_eq!(contract.security_type, SecurityType::Stock);
    }

    #[test]
    fn test_option_contracts() {
        let call = option_call("AAPL", 150.0, 2024, 12, 20);
        assert_eq!(call.symbol, Symbol::from("AAPL"));
        assert_eq!(call.security_type, SecurityType::Option);
        assert_eq!(call.right, Some(OptionRight::Call));
        assert_eq!(call.strike, 150.0);

        let put = option_put("AAPL", 150.0, 2024, 12, 20);
        assert_eq!(put.right, Some(OptionRight::Put));
    }
}
