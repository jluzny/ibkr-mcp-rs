//! F3 regression test (fault-matrix case M4): a process that cannot bind its
//! HTTP listener must exit **non-zero**.
//!
//! The defect: the bind error was logged inside the spawned server task, which
//! then completed normally, so `main` fell through to `Ok(())` and the process
//! exited 0 after logging `MCP HTTP server exited` - indistinguishable from a
//! deliberate stop, which defeats `Restart=on-failure` and hides a bridge that
//! never came up.
//!
//! The test runs the REAL binary with a port that is already held and asserts
//! (a) the typed `MCP HTTP server error` line and (b) a non-zero exit status.
//! Nothing production-facing is touched: the scratch instance gets its own
//! heartbeat/persistence paths, a dead broker port and client id 778, exactly
//! like `scripts/fault-matrix.sh` does.

use std::io::Read;
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Nothing listens on port 1, so the broker link never comes up either.
const DEAD_BROKER_PORT: u16 = 1;
/// The probe band: never the service's own client id (204).
const CLIENT_ID: &str = "778";

#[test]
fn http_bind_failure_exits_non_zero() {
    // A holder listener owns the port the binary under test will try to bind.
    let holder = TcpListener::bind("127.0.0.1:0").expect("bind the holder port");
    let taken = holder.local_addr().expect("holder addr").port();

    let scratch = std::env::temp_dir().join(format!("ibkr-mcp-f3-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).expect("create the scratch dir");

    let mut child = Command::new(env!("CARGO_BIN_EXE_ibkr-mcp-rs"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("IBKR_MCP_MCP__HOST", "127.0.0.1")
        .env("IBKR_MCP_MCP__PORT", taken.to_string())
        .env("IBKR_MCP_IBKR__HOST", "127.0.0.1")
        .env("IBKR_MCP_IBKR__PORT", DEAD_BROKER_PORT.to_string())
        .env("IBKR_MCP_IBKR__CLIENT_ID", CLIENT_ID)
        .env("IBKR_MCP_IBKR__PAPER_TRADING", "true")
        .env("IBKR_MCP_IBKR__READ_ONLY", "true")
        .env(
            "IBKR_MCP_HEARTBEAT_PATH",
            scratch.join("heartbeat").display().to_string(),
        )
        .env(
            "IBKR_MCP_PERSIST_PATH",
            scratch.join("last-good.json").display().to_string(),
        )
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the MCP binary");

    // The binary binds after its 2 s initial-connection grace period. Bound the
    // whole startup so a regression that hangs fails loudly instead of hanging
    // the suite.
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("the binary neither bound the port nor exited within 60s");
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    let mut out = String::new();
    if let Some(mut s) = child.stderr.take() {
        let _ = s.read_to_string(&mut out);
    }
    if let Some(mut s) = child.stdout.take() {
        let _ = s.read_to_string(&mut out);
    }
    let _ = std::fs::remove_dir_all(&scratch);
    drop(holder);

    assert!(
        out.contains("MCP HTTP server error"),
        "a bind failure must be surfaced as a typed ERROR line; got:\n{out}"
    );
    assert!(
        !status.success(),
        "a bind failure must exit NON-ZERO (got {status:?}): exit 0 is indistinguishable \
         from a deliberate stop, which defeats Restart=on-failure (defect F3)"
    );
    assert_eq!(
        status.code(),
        Some(1),
        "the failed-server exit code is part of the contract (got {status:?})"
    );
}
