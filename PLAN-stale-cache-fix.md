# PLAN: Fix stale account-summary cache (get_account_info serves hours-old snapshot)

**Date:** 2026-08-21
**Task:** t_8eb962eb
**Status:** For Jiri's review — DO NOT implement yet

## Problem

`get_account_info` served a 5.5-hour-old snapshot (age=20007s) while the IBKR
app showed live NLV $16.65K / +12.4% daily. MCP returned NLV $14,880 / dailyPnL
0.0 (frozen from ~07:42 CEST). Two compounding bugs cause this.

## Bug Confirmation

### BUG 1 — Cache timestamp only refreshes on `End` (lines 287-293)

The pump's inner loop handles two data variants:

- `AccountSummaryResult::Summary(s)` (line 279): inserts into the `values`
  HashMap but does **NOT** write to `state.cache` or update the timestamp.
- `AccountSummaryResult::End` (line 287): builds `AccountInfo` and writes
  `(info, Instant::now())` to the cache.

IBKR sends `End` **once** — at the end of the initial snapshot. After that,
the subscription stays open and streams individual `Summary` updates as values
change, with **no periodic `End`**. So:

1. Initial snapshot: all tags arrive as individual Summaries -> `End` -> cache
   written with `Instant::now()`.
2. NLV changes -> `Summary` update arrives -> `values` updated, but cache
   timestamp is **not** refreshed.
3. Every subsequent `get_account_info` call sees `age = now - initial_End_time`,
   which grows monotonically. After 5 minutes the cache is "stale" by the TTL
   check, and after 15 minutes it's "refused" by `stale_max` — except BUG 2
   bypasses that guard.

**CONFIRMED.** The pump is alive and streaming, but the cache timestamp is
frozen at the last `End`.

### BUG 2 — Wait loop returns stale cache without age check (lines 176-186)

```
// Step 3: wait loop
let deadline = now + 10s;
while now < deadline {
    sleep(200ms);
    if let Some((cached, age)) = self.cached() {
        return Ok(cached);  // <-- returns regardless of age
    }
}

// Step 4: stale_max guard (only reached if cache is None)
if let Some((cached, age)) = self.cached() {
    if age < self.stale_max { return Ok(cached); }
}
return Err("snapshot not received");
```

Steps 1 and 2 (lines 148-171) return early if `age < cache_ttl`. If the cache
is stale (age >= ttl), execution falls through to `fetch_lock`, re-checks,
and if still stale, calls `spawn_summary_pump()`. But the pump is already
running (CAS guard prevents a second spawn), so no new subscription opens.

The wait loop in step 3 then polls every 200ms. The cache is **non-None**
(it's just stale), so on the very first poll it returns `Ok(cached)` — a
hours-old snapshot — without any age check. The `stale_max` guard in step 4
is dead code: it's only reached when the cache is `None`, which never happens
after the initial snapshot.

**CONFIRMED.** `stale_max` (900s) is effectively never enforced.

### Combined effect

BUG 1 makes the cache look stale forever (timestamp frozen at initial `End`).
BUG 2 means the stale cache is served anyway. Result: hours-old data is
returned with no warning, no error, and no refusal.

## Proposed Fix

### Fix for BUG 1 — Refresh cache timestamp on every Summary event

In `spawn_summary_pump`, the `Summary(s)` match arm (line 279-284) must also
write to the cache:

```rust
Some(Ok(SubscriptionItem::Data(AccountSummaryResult::Summary(s)))) => {
    if acct.is_none() {
        acct = Some(s.account.clone());
    }
    values.insert(s.tag.clone(), (s.value.clone(), s.currency.clone()));

    // NEW: update cache on every Summary event so the timestamp reflects
    // the last streamed update, not just the initial End.
    // Only do this once we have enough data to build a valid AccountInfo
    // (at minimum NLV). This avoids writing partial snapshots.
    if values.contains_key(AccountSummaryTags::NET_LIQUIDATION) {
        let info = build_account_info(&values, acct.as_deref());
        *state.cache.lock().unwrap() = Some((info, Instant::now()));
    }
}
```

The `End` arm keeps its existing behavior (build + write + log "snapshot
complete"). The `End` write is now redundant with the Summary write for
post-snapshot updates, but it's still needed for the very first complete
snapshot (all tags arrive as individual Summaries, then End confirms
completeness). The Summary write ensures ongoing freshness.

**Why `contains_key(NET_LIQUIDATION)` guard?** During the initial snapshot,
Summary events arrive one tag at a time. We don't want to write a partial
cache (e.g. only NLV, no AvailableFunds). NLV is the most critical field and
arrives early; once it's present, `build_account_info` will fill the rest
from whatever tags have arrived so far (missing tags default to 0.0 via
`parse_f64`). The `End` event will then write the complete snapshot.

Alternative considered: only write on Summary **after** the first End has
been seen (track a `snapshot_complete: bool` flag). This is more conservative
but adds state. The NLV guard is simpler and sufficient — `build_account_info`
already handles missing tags gracefully.

### Fix for BUG 2 — Enforce age checks in the wait loop

The wait loop (step 3) must only return when the cache is **fresh** (age <
`cache_ttl`). If the 10s deadline expires, fall through to the stale_max
guard in step 4:

```rust
// 3. Wait for the pump to land a FRESH snapshot (bounded 10s)
let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
while tokio::time::Instant::now() < deadline {
    tokio::time::sleep(Duration::from_millis(200)).await;
    if let Some((cached, age)) = self.cached() {
        if age < self.cache_ttl {
            info!(
                account_id = %cached.account_id,
                "Fresh snapshot received after wait (age={}s)", age.as_secs()
            );
            return Ok(cached);
        }
        // Cache exists but is stale — keep waiting for the pump to refresh.
    }
}

// 4. Timed out waiting for fresh data. Serve stale if within stale_max,
//    else refuse so the margin monitor never acts on ancient numbers.
if let Some((cached, age)) = self.cached() {
    if age < self.stale_max {
        warn!(
            age_secs = age.as_secs(),
            "Wait timed out; serving stale account info (within stale_max)"
        );
        return Ok(cached);
    } else {
        warn!(
            age_secs = age.as_secs(),
            "Refusing stale account info (exceeds stale_max={}s)",
            self.stale_max.as_secs()
        );
        return Err(IbkrError::Unknown(format!(
            "account info stale (age={}s, max={}s) — pump may be disconnected or gateway down",
            age.as_secs(), self.stale_max.as_secs()
        )));
    }
}

warn!("No cache available and pump didn't deliver within 10s");
Err(IbkrError::Unknown(
    "account summary snapshot not received within 10s — gateway may be starting up".to_string(),
))
```

Key changes:
1. The wait loop checks `age < cache_ttl` before returning — stale cache is
   not returned from step 3.
2. Step 4 is now reachable (not dead code) and enforces `stale_max`.
3. Age > `stale_max` -> **error**, not stale data. The margin monitor cron
   job will see the error and can alert rather than acting on bad numbers.
4. Age between `cache_ttl` and `stale_max` -> served with a warning (pump
   may be reconnecting; stale data is better than no data for a few minutes).

### Interaction between the two fixes

With BUG 1 fixed, the pump updates the timestamp on every Summary event.
During market hours, IBKR streams NLV/AvailableFunds updates every few
seconds, so the cache stays fresh and `get_account_info` returns immediately
from step 1 or 2.

Outside market hours or on a quiet account, no Summary events arrive, so the
timestamp ages. After 5 min (TTL), the next call enters the wait loop. If no
Summary arrives within 10s (quiet market), the stale_max guard kicks in:
serve with warning if < 15 min, refuse if > 15 min.

This is correct behavior: during off-hours, the account values don't change,
so stale data within 15 min is fine. Beyond 15 min, something is likely wrong
(pump died, gateway disconnected) and refusing is safer.

## Files to Change

| File | Lines | Change |
|------|-------|--------|
| `src/ibkr/account.rs` | 279-284 | Add cache write on `Summary` event (BUG 1 fix) |
| `src/ibkr/account.rs` | 176-201 | Add age check in wait loop + enforce stale_max (BUG 2 fix) |

No new files, no struct changes, no API changes. ~15 lines of diff.

## Test Plan

### Unit tests (in `account_tests.rs`)

1. **test_cache_freshness_on_summary_update**: Simulate a Summary event
   arriving after the initial End. Verify the cache timestamp is updated
   (age resets to ~0), not frozen at the End timestamp.

2. **test_wait_loop_refuses_stale_cache**: Set up a cache with age >
   stale_max. Call `get_account_info` with a mock pump that never refreshes.
   Verify it returns `Err` (not `Ok` with stale data).

3. **test_wait_loop_serves_stale_within_stale_max**: Cache age between
   `cache_ttl` and `stale_max`. Verify it returns `Ok` with a warning after
   the 10s wait.

4. **test_wait_loop_returns_fresh_after_refresh**: Cache starts stale, pump
   delivers fresh data during the 10s wait. Verify it returns `Ok` with
   fresh data (age < ttl) before the deadline.

### Integration / e2e tests (manual, against live gateway)

1. **Fresh data after forced refresh**: Restart `ibkr-mcp` (clears cache).
   Call `get_account_info` 3x rapidly. First call should wait for the
   snapshot (age < 10s). Second/third should serve from cache (age < 300s).
   Verify NLV matches the IBKR mobile app.

2. **Stale refusal**: Stop the pump (kill the background task or disconnect
   the gateway). Wait > 15 min. Call `get_account_info`. Must return an
   error mentioning "stale" and the age. The margin monitor cron job should
   see this error and not act on old numbers.

3. **Streaming freshness**: During market hours, call `get_account_info`
   every 30s for 10 minutes. Every call should return age < 300s (fresh),
   proving the Summary updates are refreshing the timestamp. NLV should
   change as positions fluctuate.

4. **journalctl verification**: After deploying, check logs for:
   - "Pump: snapshot complete, cache updated" (initial End)
   - No "Snapshot received after wait (age=NNNNNs)" with age > 300s
   - "Fresh snapshot received after wait" (when cache was stale and pump
     refreshed it within 10s)

### What NOT to test

- `dailyPnL` — hardcoded 0.0, known gap, belongs to task t_1e63b08f
- `get_positions` PnL fields — separate task t_2d76b9be

## Deployment

**DO NOT DEPLOY.** This plan is for Jiri's review. After approval:
1. Implement the two fixes in `src/ibkr/account.rs`.
2. Build: `cd /home/jiri/dev/trading/ibkr-mcp-rs && CARGO_TARGET_DIR=/tmp/ibkr-target cargo build --release --bin ibkr-mcp-rs`
3. Run unit tests: `cargo test --lib account`
4. Jiri reviews the diff.
5. Deploy: copy binary to /usr/local/bin/ and reload the ibkr-mcp service.
6. Run e2e test #1 (fresh data after reload).
