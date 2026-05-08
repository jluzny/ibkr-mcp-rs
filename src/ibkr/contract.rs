use ibapi::prelude::*;

/// Build a stock contract
pub fn stock(symbol: &str) -> Contract {
    Contract::stock(symbol).build()
}

/// Build an option contract
pub fn option_call(symbol: &str, strike: f64, year: i32, month: u32, day: u32) -> Contract {
    Contract::call(symbol)
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
}
