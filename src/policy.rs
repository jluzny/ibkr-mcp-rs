//! The D5 flag gate — `ibkr.read_only` / `ibkr.paper_trading` enforcement.
//!
//! Ported from `ibkr-mcp-py`'s `policy.OrderGate` (the parity contract in
//! `ibkr-mcp-py/docs/DEVIATIONS.md` §D5 rows 1–2, D4-2): an explicit
//! `read_only: true` refuses the order-path tool `what_if`; an explicit
//! `paper_trading: true` refuses a non-paper `account_id` on the
//! account-scoped tools.
//!
//! **Effectiveness follows provenance.** Both flags default to `true` in the
//! built-in config, so taking a built-in default at face value would refuse
//! tools in a default-config run. A flag is therefore live only when an
//! operator set it explicitly (`Config::explicit_flags`) AND set it to `true`.
//!
//! The refusal strings are contract: the Python side asserts them verbatim.
//! They travel in the tool's own error envelope
//! (`{"success": false, "error": <refusal>}`).

/// Tools with an order path: the only tools `ibkr.read_only` refuses.
pub const ORDER_PATH_TOOLS: &[&str] = &["what_if"];

/// Tools that take an `account_id` and can therefore be pointed at the wrong
/// account. D4 adds `get_open_orders` as the fifth.
pub const ACCOUNT_SCOPED_TOOLS: &[&str] = &[
    "get_account_info",
    "get_positions",
    "get_executions",
    "what_if",
    "get_open_orders",
];

/// IBKR's own paper-account convention, measured on the deployed pair: the
/// paper account is `DU…` (`DUK271345` on 8882) vs live `U…` (`U18197748`).
pub const PAPER_ACCOUNT_PREFIX: &str = "DU";

/// `true` for an IBKR paper account id (`DU…`).
pub fn is_paper_account(account_id: &str) -> bool {
    account_id.trim().to_uppercase().starts_with(PAPER_ACCOUNT_PREFIX)
}

/// The D5 flag gate. Built from the loaded config's *effective* flags, never
/// from built-in defaults (see the module docs).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderGate {
    /// Effective `ibkr.read_only` (explicitly configured `true`).
    pub read_only: bool,
    /// Effective `ibkr.paper_trading` (explicitly configured `true`).
    pub paper_trading: bool,
}

impl OrderGate {
    /// The gate as the loaded config makes it effective: a flag is live only
    /// when it was set explicitly *and* is `true`.
    pub fn from_config(config: &crate::config::Config) -> Self {
        Self {
            read_only: config.ibkr.read_only && config.explicit_flags.read_only,
            paper_trading: config.ibkr.paper_trading && config.explicit_flags.paper_trading,
        }
    }

    /// The refusal for this call, or `None` when the call is allowed.
    ///
    /// `account_id` is the *requested* account (`None` = omitted). Mirrors the
    /// Python dispatch exactly: an omitted account is never refused, and the
    /// read-only refusal (a decision about the tool) takes precedence over the
    /// account refusal (a decision about the call).
    pub fn refusal_for(&self, tool: &str, account_id: Option<&str>) -> Option<String> {
        if self.read_only && ORDER_PATH_TOOLS.contains(&tool) {
            return Some(format!(
                "refused: ibkr.read_only is set to true (explicitly configured), so the order-path tool {tool} is not available"
            ));
        }
        if self.paper_trading && ACCOUNT_SCOPED_TOOLS.contains(&tool) {
            if let Some(account_id) = account_id {
                if !is_paper_account(account_id) {
                    return Some(format!(
                        "refused: ibkr.paper_trading is set to true (explicitly configured) and account_id={account_id} is not a paper account, so {tool} will not act on it"
                    ));
                }
            }
        }
        None
    }

    /// `true` when the call is refused.
    pub fn refuses(&self, tool: &str, account_id: Option<&str>) -> bool {
        self.refusal_for(tool, account_id).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(read_only: bool, paper_trading: bool) -> OrderGate {
        OrderGate {
            read_only,
            paper_trading,
        }
    }

    #[test]
    fn a_default_gate_refuses_nothing() {
        let g = OrderGate::default();
        for tool in ACCOUNT_SCOPED_TOOLS {
            assert!(!g.refuses(tool, Some("U18197748")), "{tool} must stay open");
            assert!(!g.refuses(tool, None), "{tool} must stay open");
        }
    }

    #[test]
    fn read_only_refuses_what_if_and_only_what_if() {
        let g = gate(true, false);
        let refusal = g.refusal_for("what_if", None).expect("what_if must be refused");
        assert_eq!(
            refusal,
            "refused: ibkr.read_only is set to true (explicitly configured), so the order-path tool what_if is not available"
        );
        // The read tools are never refused by read_only.
        for tool in ["get_account_info", "get_positions", "get_executions", "get_open_orders"] {
            assert!(!g.refuses(tool, Some("U18197748")), "{tool} must stay open");
        }
    }

    #[test]
    fn paper_trading_refuses_a_non_paper_account_on_the_scoped_tools() {
        let g = gate(false, true);
        for tool in ACCOUNT_SCOPED_TOOLS {
            let refusal = g
                .refusal_for(tool, Some("U18197748"))
                .unwrap_or_else(|| panic!("{tool} must refuse a live account"));
            assert_eq!(
                refusal,
                format!(
                    "refused: ibkr.paper_trading is set to true (explicitly configured) and account_id=U18197748 is not a paper account, so {tool} will not act on it"
                )
            );
        }
        // Omitted account and the paper account keep working.
        for tool in ACCOUNT_SCOPED_TOOLS {
            assert!(!g.refuses(tool, None), "{tool} must accept an omitted account");
            assert!(!g.refuses(tool, Some("DUK271345")), "{tool} must accept a paper account");
        }
    }

    #[test]
    fn paper_trading_never_refuses_a_non_scoped_tool() {
        let g = gate(false, true);
        for tool in ["get_market_data", "get_bulk_quotes", "run_scanner", "get_events"] {
            assert!(!g.refuses(tool, Some("U18197748")), "{tool} is not account-scoped");
        }
    }

    #[test]
    fn read_only_takes_precedence_over_the_account_refusal() {
        let g = gate(true, true);
        let refusal = g.refusal_for("what_if", Some("U18197748")).unwrap();
        assert!(refusal.starts_with("refused: ibkr.read_only"), "{refusal}");
    }

    #[test]
    fn paper_account_matching_is_trimmed_and_case_insensitive() {
        assert!(is_paper_account("DUK271345"));
        assert!(is_paper_account("duk271345"));
        assert!(is_paper_account("  DUK271345  "));
        assert!(!is_paper_account("U18197748"));
        assert!(!is_paper_account(""));
    }

    /// Pinned Python quirk (parity): the dispatch passes the *requested*
    /// account through, so an explicitly empty `account_id` is not the same as
    /// an omitted one — `is_paper_account("")` is false, and the refusal fires.
    #[test]
    fn an_explicitly_empty_account_id_is_refused_like_any_non_paper_id() {
        let g = gate(false, true);
        assert!(g.refuses("get_positions", Some("")));
    }
}
