# PLAN: owning-installer support for the paper bridge unit

**Status: APPROVED ("y", 2026-10-11) and IMPLEMENTED in `scripts/install-mcp-tuple.sh`**
as `--unit=live|paper` (a semantic selector rather than `--unit-name=<literal>` —
same allowlist guarantee, less typo surface). One owning installer serves both units.
Gate-qualified: paper manifest **36 ok / 1 warn (dirty tree) / 0 fail**, live regression
manifest identical to the qualified tuple. Findings that reshaped the estimate: the
record at the end of this file.
**Date: 2026-10-11. Author: Hermes (trader profile).**

## Problem

`scripts/install-mcp-tuple.sh` is the ONE owning installer for the ibkr-mcp tuple
(PLAN-gateway-supervisor-refactor.md §6.3, §13), but `UNIT_NAME=ibkr-mcp.service` is
hard-coded. The paper bridge unit `systemd/ibkr-mcp-paper.service` (8882 → raw 4002,
client id 205, per-instance state `/var/lib/ibkr-mcp-paper`) therefore has **no owning
installer at all.

`check-mcp-policy.sh` enforces "exactly one owning installer per installed unit" and
"no superseded artifact may be installed over the installed binary". A hand-installed
paper unit would fail that gate — which is the gate working as intended.

The live tuple is already qualified and gate-green (35 ok / 0 fail at `8095e4e`), so this
blocks **only the paper bridge**, not the critical live restore.

## Options

### A (recommended): `--unit-name=` on `install-mcp-tuple.sh`

Add a flag, allowlisted to exactly `ibkr-mcp.service` and `ibkr-mcp-paper.service`
(no arbitrary unit names — the installer must never become a generic unit writer).
Derive `UNIT_DEST`, `UNIT_SRC`, `DROPIN_SRC`, `DROPIN_DEST(_DIR)` from it; everything
else (snapshot-rollback, manifest, gate, install, verify, rollback) is unchanged.

Work items:
1. `--unit-name=<name>` parsing + allowlist; reject anything else with `die`.
2. Manifest per-unit identity that is currently hard-coded to the live unit:
   `persist_path`/`heartbeat_path` are emitted as `/tmp/ibkr-mcp-last-good.json` +
   `/tmp/ibkr-mcp-heartbeat`; the paper unit runs with `IBKR_MCP_PERSIST_PATH` under
   `/var/lib/ibkr-mcp-paper`. Read the paths from the selected unit's `Environment=`
   (same pattern already used for `client_id` / `broker_port`), falling back to the
   live defaults.
3. Rollback generation pair is per-unit ownership: the paper tuple's first deployment
   records `1/0` exactly like the live one did.
4. Verify `check-mcp-policy.sh` accepts a paper-tuple manifest (its "exactly one
   installer owns <unit>" scan must resolve the paper basename to this script) and run
   its `--self-test` unchanged.
5. Tests: extend `scripts/verify-outage-policy.sh` with the paper tuple; the gate run
   on the generated paper manifest must reach 0 fail before any install.

Not changed: guard patterns `(gateway|ibgw-ibc)`, `GATEWAY_UNITS`, the no-restart /
no-credential-refusal rules, the park semantics.

### B (rejected): install the paper unit by hand

Fails the policy gate by design (no owning installer). Would need a gate exception,
which weakens the invariant for every future tuple.

### C (rejected): fold the unit into the pacman package

Breaks the ownership contract: pacman owns the binary (`/usr/local/bin/ibkr-mcp-rs`),
the repo owns units + drop-ins. The gate's package-provenance section assumes this split.

## Resolution + implementation record (2026-10-11)

Both questions answered "y" — approved, paper joins this run. What the code
actually needed (the "~40 lines" estimate was wrong; ~120, and three findings
reshaped the design):

1. **The gate's unit-ownership analyzer is a static reader.** It attributes an
   install only through verbs (`install|cp|mv|ln|tee|cat`) whose destination
   words resolve via the script's *simple assignments*. So both units need
   their own concrete literal install words — `UNIT_DEST` and `PAPER_UNIT_DEST`
   branches in every install/restore verb — or the gate cannot prove "exactly
   one installer owns ibkr-mcp-paper.service".
2. **First-install rollback shape.** The gate requires `rollback_mode` ∈
   {park, restore_previous} and a non-empty materialized package with declared
   checksums — but the paper tuple has *no predecessor*. Shipped shape: package =
   shared pre-deploy binary + config (no unit slot), `rollback_unit_sha256=none`
   (a sentinel that can never pass for a checksum), park reason spelling out the
   semantics: a paper rollback **removes the unit** (restores absence) and never
   touches the shared binary/config (those belong to the replaced tuple's own
   rollback). No gate change was needed.
3. **`client_id_migration_ref` is mandatory for any id ≠ 204** (section 6.3 step 7).
   The paper tuple declares 205's migration reference automatically (override:
   `CLIENT_ID_MIGRATION_REF='<ref>'`).

Plus one bug class worth remembering: `sha256_of` is a `sha256sum|cut` pipeline
under `pipefail`, so a *missing* file makes a bare `v=$(sha256_of …)` assignment
abort the whole script under `set -e` **silently**. Fixed at all assignment sites
(`|| true`); the same latent bug in `mode_verify`'s `reported=$(curl … | sed …)`
is fixed too — evidence collection must survive a down bridge.

Verified: paper manifest **36 ok / 1 warn (dirty tree) / 0 fail**; live regression
manifest byte-identical identity to the qualified tuple (client 204, /tmp state
paths, `rollback_unit_sha256=16bdb837…` = the legacy unit).
