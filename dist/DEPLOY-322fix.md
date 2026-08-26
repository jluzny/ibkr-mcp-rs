# Deploy the 322 fix for ibkr-mcp-rs (2026-08-17)

The fixed binary is staged and verified. Two commands to deploy (root required):

    sudo install -m 755 /home/jiri/dev/trading/ibkr-mcp-rs/dist/ibkr-mcp-rs-322fix /usr/local/bin/ibkr-mcp-rs
    sudo systemctl restart ibkr-mcp.service

Why the service bounce: the running process still holds 3 leaked
account-summary slots on the current gateway connection; the restart
clears them and starts the new binary with the single-subscription pump.

Verify after deploy:

    journalctl -u ibkr-mcp.service --no-pager -n 20 | grep -E 'Pump|connected'
    # expect: "IBKR connected successfully", then on first get_account_info:
    # "Pump: opening account summary subscription" ... "Pump: snapshot complete"

Source commit: 2ba05d7 on feat/option-greeks (src/ibkr/account.rs).
Build artifact: dist/ibkr-mcp-rs-322fix (built with CARGO_TARGET_DIR=/tmp/ibkr-target).
