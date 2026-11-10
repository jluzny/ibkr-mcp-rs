# Account summary streaming: provenance, binding, and refresh policy

This note describes the account-summary changes published together in this
tree. It is documentation only; it does not add tool surface and it makes no
claim about runtime qualification, freshness guarantees, or test results
beyond what the accompanying commit actually contains.

## 1. rust-ibapi 5.1.0

The manifest (`Cargo.toml`) pins the client SDK to the `v5.1.0` tag of
`rust-ibapi`; `Cargo.lock` records the resolved revision for that tag. No
other dependency or toolchain change is part of this work.

## 2. Fail-closed completeness markers, counts, and provenance

Every account-summary, positions, and executions read now carries a
provenance record alongside the data:

- `end_marker` / `end` — the terminal marker the broker actually sent
  (`AccountSummaryEnd`, `PositionEnd`, `ExecutionDataEnd`), or `none` when the
  read finished without proving it.
- `terminal_sentinel` — whether the read observed its protocol terminator.
- `count` — the number of elements emitted in this payload, never the number
  of frames seen before filtering.
- `generation` — a monotonic read/subscription epoch: one value per
  account-summary subscription and per one-shot positions/executions read;
  account payloads published within one long-lived subscription share a
  generation. Payloads with different generations must not be merged
  field-by-field.
- `received_at` — the RFC 3339 UTC instant the payload was finalized.

The intent is fail-closed: a consumer can tell a truncated or unproven read
from a complete one instead of inferring completeness from an empty tail.
Positions additionally report the pre-filter frame count (`received_count`)
and whether every emitted mark came from a per-position PnL reply
(`marks_attested`). Absent account tags stay JSON `null`; they are never
coerced to `0.0`.

## 3. Account binding and per-field receipts

`get_account_info` returns the broker's `accountId` and sets
`account_bound: true` only when that label is non-empty. When the caller names
an `account_id`, the bridge compares it with the label the broker returned:

- a match adds `requestedAccountId` to the payload;
- a mismatch fails closed with `success: false` and does not echo the account
  or claim `account_bound`, because the bridge keeps a single `"All"`
  subscription and cannot filter per account.

Each known summary field is accompanied by:

- `presence` — whether the broker carried the tag at all;
- `field_receipts` — the last time that tag was actually received;
- `tag_currency` — the currency IBKR reported for the tag, when present.

These tables are keyed by payload field name, not raw IBKR tag id, so a
consumer can prove per field whether a value exists and how old it is.

## 4. Optional bounded refresh, retirement confirmation, and 322 safety

IBKR does not resend unchanged summary values on a long-lived subscription.
The bridge therefore supports an optional periodic refresh
(`ibkr.account_refresh_secs`, default `60` seconds): the pump cancels the
current `AccountSummary` request and opens exactly one replacement, at most
once per interval. `0` disables the refresh entirely.

Replacement is gated on retirement confirmation. IBKR never acknowledges
`CancelAccountSummary`, so the bridge waits briefly for the SDK to report the
cancelled subscription terminal. Only that local proof allows a replacement
to be opened; a subscription without a request id is treated as still
registered.

If IBKR answers a replacement with `[322]` (the account-summary cap), that is
treated as fail-closed evidence that the previous request may still be
registered (a cap error can also reflect another outstanding request occupying
a slot). The pump then latches `summary_slot_leaked`, parks, and does not open
another subscription for the life of the process. The same parking applies to
an unconfirmed retirement or to a bounded run of open/stream failures. While
parked, reads still return the last snapshot but fail closed on freshness: the
payload keeps `summary_slot_leaked` truthful (true only after `[322]`) and
carries the real broker error text in `pump_error` instead of a generic "pump
may be down" message. Only a process restart re-arms the pump.

## 5. Durable live-service policy: refresh disabled

The default periodic refresh is intended for deployments that want the
per-tag receipts to be restamped. The shipped live service unit takes the
opposite, conservative choice and sets:

```
IBKR_MCP_IBKR__ACCOUNT_REFRESH_SECS=0
```

With refresh disabled, the bridge keeps one long-lived subscription and never
cancels/reopens it, so it cannot burn the `[322]` cap through refresh churn.
Background subscription timestamps therefore remain genuine last-update
times: they record when IBKR last sent a value and are not synthetic
heartbeats. Receipt ages grow when the broker does not resend a tag; that is
the signal consumers should read, and it is not evidence of a stalled or
failed bridge.

The override is deliberate and deployment-specific. It does not imply that
every runtime is qualified or that account data is always fresh; it documents
the policy the live unit ships with.
