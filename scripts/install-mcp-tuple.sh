#!/usr/bin/env bash
# =============================================================================
# install-mcp-tuple.sh - the ONE owning installer for the ibkr-mcp tuple
# =============================================================================
#
# PLAN-gateway-supervisor-refactor.md §6.3 (atomic qualified deployment), §13
# (one repository owns each installed unit) and §14 Phase 1.
#
# The qualified tuple is: source snapshot + Cargo.lock + toolchain + binary +
# systemd unit + config + client id + persistence/heartbeat paths + health
# contract version, all recorded in an artifact manifest and validated by
# ibkr-stack/scripts/check-mcp-policy.sh BEFORE anything is installed. This
# script is the only thing allowed to put that tuple on the host:
#
#   * it never starts, stops or restarts a service (a restart drops the live
#     IBKR session and re-handshakes client id 204, so it is a separate,
#     explicitly approved action - `--mode=rollback` is the only mode that
#     touches the service, and it requires --approved);
#   * it never starts, stops or restarts the Gateway, and it refuses to install
#     a unit that carries a Gateway ACTIVATION edge (Requires=/Wants=/BindsTo=/
#     PartOf=/Requisite=/Upholds=);
#   * it never writes to .env or any credential file;
#   * it materializes the pre-install artifacts as a rollback package before it
#     overwrites anything, and writes a dated copy of what it replaced.
#
# Modes:
#   --mode=check              read-only. Verify the manifest against the
#                             artifacts on disk, the unit policy, the config and
#                             the rollback package. Exit 0 = consistent.
#   --mode=manifest           print a resolved manifest for the current tree
#                             (all 27 required keys, rollback checksums
#                             included). Redirect it to a file and validate it
#                             with check-mcp-policy.sh.
#   --mode=snapshot-rollback  materialize the pre-install rollback package
#                             (binary + unit + config of what is installed
#                             NOW) into the manifest's rollback_dir. Run BEFORE
#                             --mode=manifest. Needs root to read the installed
#                             unit; pass --rollback-dir to override the path.
#   --mode=install            root. Verify, write a dated backup of what it
#                             replaces, install binary + unit + config
#                             atomically, daemon-reload, enable, then prove the
#                             effective graph has no Gateway edge. NO restart.
#   --mode=unit               root. Unit file only (same verification).
#   --mode=dropin             root. Restart-policy drop-in only.
#   --mode=verify             read-only. Post-install/post-restart proof:
#                             artifact hashes vs the manifest, effective graph,
#                             running-process identity, health contract version,
#                             client id, Gateway invocation identity.
#   --mode=rollback           root + --approved. Stop the service, install one
#                             complete tuple from a package directory, start it
#                             again and verify. --restore=package uses the
#                             rollback package (the pre-install tuple);
#                             --restore=forward uses the forward package this
#                             script wrote at install time.
#
# A rollback restores a COMPLETE tuple, never single files, and it never
# reinstates a Gateway activation edge: if the package's unit carries one, the
# unit is PARKED (not restored) and the reason is printed. §6.3 step 9.
#
# Config artifact: config/default.yaml is deployed in place - the unit runs with
# WorkingDirectory=<repo>, and the binary reads config/{env}.yaml from there. So
# "installing" the config means: its bytes must hash to the manifest value
# before and after the install, and the pre-install copy is preserved in the
# rollback package. Every other artifact is copied onto its install path.
#
# Usage:
#   cd ~/dev/trading/ibkr-mcp-rs
#   --unit=live|paper  selects the tuple (default live): ibkr-mcp.service
#                      (8881 -> 4001, client 204) or ibkr-mcp-paper.service
#                      (8882 -> 4002, client 205). One owning installer, both units.
#   bash scripts/install-mcp-tuple.sh --mode=snapshot-rollback [--unit=paper]
#   bash scripts/install-mcp-tuple.sh --mode=manifest --binary <qualified build> > /tmp/tuple.properties
#     (ROLLBACK_NEW_GENERATION / ROLLBACK_PREVIOUS_GENERATION override the
#      recorded ownership generation; the default 1/0 is the first deployment.
#      A redeployment must record the strictly increasing pair.)
#   bash ~/dev/trading/ibkr-stack/scripts/check-mcp-policy.sh --manifest /tmp/tuple.properties --installed
#   sudo bash scripts/install-mcp-tuple.sh --mode=install --manifest /tmp/tuple.properties
#   bash scripts/install-mcp-tuple.sh --mode=verify   --manifest /tmp/tuple.properties
set -Eeuo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
UNIT_NAME=ibkr-mcp.service
UNIT_DEST=/etc/systemd/system/$UNIT_NAME
DROPIN_DEST_DIR=/etc/systemd/system/$UNIT_NAME.d
DROPIN_DEST=$DROPIN_DEST_DIR/10-restart-policy.conf
INSTALL_BIN_DEST=/usr/local/bin/ibkr-mcp-rs
UNIT_SRC=$REPO_ROOT/systemd/$UNIT_NAME
DROPIN_SRC=$REPO_ROOT/systemd/$UNIT_NAME.d/10-restart-policy.conf
ROLLBACK_DIR_DEFAULT=$REPO_ROOT/backups/mcp-phase1
FORWARD_DIR_DEFAULT=$REPO_ROOT/backups/mcp-phase1-forward
HEALTH_URL_DEFAULT=http://127.0.0.1:8881/health/live
GATEWAY_UNITS="ibgw-ibc@live.service ibgw-ibc@paper.service"

# --- unit selection: ONE owning installer serves BOTH tuples -----------------
# `--unit=live` (default) selects ibkr-mcp.service (8881 -> 4001, client 204).
# `--unit=paper` selects ibkr-mcp-paper.service (8882 -> 4002, client 205).
# The paper unit carries its restart policy INLINE (Restart=always, RestartSec=30,
# StartLimit 300/5) - it has no drop-in. The parallel PAPER_* assignments below
# are load-bearing for the policy gate's STATIC unit-ownership analyzer, which
# resolves install/mv destinations through simple assignments only: every unit
# this installer owns needs its own literal words here, or the gate cannot prove
# that exactly one installer owns it.
UNIT_SEL=live
PAPER_UNIT_NAME=ibkr-mcp-paper.service
PAPER_UNIT_DEST=/etc/systemd/system/$PAPER_UNIT_NAME
PAPER_UNIT_SRC=$REPO_ROOT/systemd/$PAPER_UNIT_NAME

MODE=check
MANIFEST=${MANIFEST:-}
ARTIFACT_BIN_PATH=${ARTIFACT_BIN_PATH:-}
ROLLBACK_DIR_OVERRIDE=${ROLLBACK_DIR_OVERRIDE:-}
FROM_DIR=
RESTORE=package
APPROVED=0
FORCE=0

# --help prints this header verbatim: everything between line 2 and the first
# `set -Eeuo` line.
usage() { awk 'NR>1 && /^set -Eeuo/{exit} NR>1{print}' "${BASH_SOURCE[0]}"; exit "${1:-0}"; }

# Accepts both --key=value and --key value.
while [ $# -gt 0 ]; do
  case "$1" in
    --mode=*) MODE="${1#--mode=}" ;;
    --mode) shift; MODE="${1:-}" ;;
    --manifest=*) MANIFEST="${1#--manifest=}" ;;
    --manifest) shift; MANIFEST="${1:-}" ;;
    --binary=*) ARTIFACT_BIN_PATH="${1#--binary=}" ;;
    --binary) shift; ARTIFACT_BIN_PATH="${1:-}" ;;
    --rollback-dir=*) ROLLBACK_DIR_OVERRIDE="${1#--rollback-dir=}" ;;
    --rollback-dir) shift; ROLLBACK_DIR_OVERRIDE="${1:-}" ;;
    --unit=*) UNIT_SEL="${1#--unit=}" ;;
    --unit) shift; UNIT_SEL="${1:-}" ;;
    --from=*) FROM_DIR="${1#--from=}" ;;
    --from) shift; FROM_DIR="${1:-}" ;;
    --restore=package|--restore=forward) RESTORE="${1#--restore=}" ;;
    --restore) shift; RESTORE="${1:-}" ;;
    --approved) APPROVED=1 ;;
    --force) FORCE=1 ;;
    -h|--help) usage 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage 2 ;;
  esac
  shift
done

case "$MODE" in
  check|manifest|snapshot-rollback|install|unit|dropin|verify|rollback) ;;
  *) echo "error: unknown mode: ${MODE:-<empty>}" >&2; usage 2 ;;
esac
case "$RESTORE" in
  package|forward) ;;
  *) echo "error: --restore must be package or forward" >&2; usage 2 ;;
esac
case "$UNIT_SEL" in
  live|paper) ;;
  *) echo "error: --unit must be live or paper (got: ${UNIT_SEL:-<empty>})" >&2; usage 2 ;;
esac
# SELECTED tuple's read paths. The install verbs below keep the literal
# $UNIT_* / $PAPER_UNIT_* words (see the ownership-analyzer note above); these
# SEL_* values are only ever read, never used as install destinations.
SEL_UNIT_NAME=$UNIT_NAME
SEL_UNIT_SRC=$UNIT_SRC
SEL_UNIT_DEST=$UNIT_DEST
SEL_DROPIN_SRC=$DROPIN_SRC
SEL_DROPIN_DEST=$DROPIN_DEST
if [ "$UNIT_SEL" = paper ]; then
  SEL_UNIT_NAME=$PAPER_UNIT_NAME
  SEL_UNIT_SRC=$PAPER_UNIT_SRC
  SEL_UNIT_DEST=$PAPER_UNIT_DEST
  SEL_DROPIN_SRC=
  SEL_DROPIN_DEST=
  # The paper bridge answers its health endpoints on 8882.
  HEALTH_URL_DEFAULT=http://127.0.0.1:8882/health/live
fi

say()  { printf '%s\n' "$*"; }
step() { printf '\n== %s ==\n' "$*"; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

FAILS=0
ok()   { printf '  ok    %s\n' "$*"; }
bad()  { printf '  FAIL  %s\n' "$*"; FAILS=$((FAILS + 1)); }
warn() { printf '  WARN  %s\n' "$*"; }

sha256_of() { sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }

# Scratch files live in a private mktemp path, never a fixed shared one. A fixed
# path such as /tmp/.mcp-tuple-health.json is world-writable-but-sticky: if it is
# owned by another uid, curl cannot write it, `curl -o ... || printf 'ERR '`
# prints ERR and then dumps the PREVIOUS run's body as if it were the answer.
# Evidence must never be able to display a stale body.
TMP_FILES=()
tmp_file() {  # tmp_file <label> -> path (created, empty, private)
  local f
  f="$(mktemp "${TMPDIR:-/tmp}/mcp-tuple-$1.XXXXXX")" || die "mktemp failed"
  TMP_FILES+=("$f")
  printf '%s' "$f"
}
cleanup_tmp_files() {
  local f
  for f in "${TMP_FILES[@]:-}"; do
    [ -n "$f" ] && rm -f "$f"
  done
  # An EXIT trap that returns non-zero REPLACES the script's exit status, which
  # would turn a successful run into an apparent failure.
  return 0
}
trap cleanup_tmp_files EXIT

# Same computation as check-mcp-policy.sh (the gate is the authority):
#   find src tests systemd config scripts Cargo.toml Cargo.lock -type f \
#     ! -path '*/__pycache__/*' ! -name '*.pyc' ! -name '*.pyo' ! -name '*.pyd' \
#     | LC_ALL=C sort | xargs sha256sum | sha256sum
# Three load-bearing properties (measured broken once already, findings F4/F7):
# `docs` is NOT a hashed root (evidence is written after emission and must not
# invalidate the tuple), generated bytecode is NOT hashed (appears by merely
# running the bridge), and the collation is PINNED to C (ambient en_US.UTF-8
# sort reorders filenames and hashes the same tree differently).
snapshot_tree_hash() {
  local listing
  listing="$(cd "$REPO_ROOT" && find src tests systemd config scripts Cargo.toml Cargo.lock -type f \
      ! -path '*/__pycache__/*' ! -name '*.pyc' ! -name '*.pyo' ! -name '*.pyd' 2>/dev/null \
    | LC_ALL=C sort || true)"
  [ -n "$listing" ] || { printf 'no-source-files'; return 0; }
  ( cd "$REPO_ROOT" && printf '%s\n' "$listing" | xargs -r sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1 ) \
    || printf 'hash-failed'
}

manifest_value() {
  [ -n "$MANIFEST" ] && [ -f "$MANIFEST" ] || return 0
  sed -n "s/^[[:space:]]*$1[[:space:]]*=[[:space:]]*//p" "$MANIFEST" | tail -1 | tr -d '\r' | sed -e 's/^"//' -e 's/"$//'
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    step "re-executing under sudo (mode=$MODE)"
    exec sudo -- "$0" "$@"
  fi
}

# --- rollback / forward package helpers --------------------------------------
# A package directory holds up to three files, named after the role they play
# (the validator maps them by suffix): the unit (*service), the config (*.yaml)
# and the binary (anything else). A FIRST-INSTALL capture has no unit slot: the
# tuple's unit never existed on this host, so only the shared binary and config
# are materialized (and the manifest declares rollback_unit_sha256=none).
package_unit()   { printf '%s/%s' "$1" "$SEL_UNIT_NAME"; }
package_config() { printf '%s/default.yaml' "$1"; }
package_bin()    { printf '%s/ibkr-mcp-rs' "$1"; }

package_hashes() {
  local dir="$1"
  printf 'unit=%s\n'   "$(sha256_of "$(package_unit "$dir")" 2>/dev/null || true)"
  printf 'config=%s\n' "$(sha256_of "$(package_config "$dir")" 2>/dev/null || true)"
  printf 'binary=%s\n' "$(sha256_of "$(package_bin "$dir")" 2>/dev/null || true)"
}

write_package() {
  # write_package <dir> <binary-src> <unit-src> <config-src>
  # An EMPTY <unit-src> is the first-install capture described above.
  local dir="$1" bsrc="$2" usrc="$3" csrc="$4"
  mkdir -p "$dir"
  cp -a "$bsrc" "$(package_bin "$dir")"
  if [ -n "$usrc" ]; then
    cp -a "$usrc" "$(package_unit "$dir")"
  fi
  cp -a "$csrc" "$(package_config "$dir")"
}

# --- shared verification ------------------------------------------------------
verify_manifest_against_host() {
  # 1. every artifact of the tuple hashes to its declared value
  local path declared actual
  for pair in "binary_path:binary_sha256" "unit_path:unit_sha256" "config_path:config_sha256"; do
    path="$(manifest_value "${pair%%:*}")"
    declared="$(manifest_value "${pair##*:}")"
    [ -n "$path" ] && [ -f "$path" ] || { bad "manifest ${pair%%:*} does not exist: ${path:-<none>}"; continue; }
    actual="$(sha256_of "$path")"
    [ "$actual" = "$declared" ] && ok "${pair##*:} matches $(basename "$path")" \
      || bad "${pair##*:} mismatch for $path (declared $declared, on disk $actual)"
  done

  # 2. source identity
  local snap
  snap="$(snapshot_tree_hash)"
  [ "$snap" = "$(manifest_value snapshot_tree_sha256)" ] \
    && ok "snapshot_tree_sha256 matches the source tree" \
    || bad "snapshot_tree_sha256 mismatch (declared $(manifest_value snapshot_tree_sha256), computed $snap)"

  # 3. unit policy: no Gateway activation edge, bounded restarts
  local unit
  unit="$(manifest_value unit_path)"
  if [ -f "$unit" ]; then
    if grep -nE '^[[:space:]]*(Requires|Wants|BindsTo|PartOf|Requisite|Upholds)[[:space:]]*=.*(gateway|ibgw-ibc)' "$unit"; then
      bad "unit declares a Gateway ACTIVATION edge - refusing (plan §6.1)"
    else
      ok "unit has no Gateway activation edge (Requires=/Wants=/...)"
    fi
    grep -qE '^[[:space:]]*StartLimitBurst[[:space:]]*=' "$unit" \
      && ok "unit bounds restarts (StartLimitBurst)" || bad "unit has no StartLimitBurst="
    grep -qE '^[[:space:]]*RestartSec[[:space:]]*=[[:space:]]*(30|3[0-9]|[4-9][0-9]|[0-9]{3,})' "$unit" \
      && ok "unit RestartSec is >= 30s" || warn "unit RestartSec is below the qualified 30s"
  else
    bad "unit_path missing: ${unit:-<none>}"
  fi

  # 4. the contract version must agree in manifest, config and unit
  local version config declared_version unit_version
  version="$(manifest_value health_contract_version)"
  case "$version" in
    legacy-v0|lease-v1) ok "manifest declares health_contract_version=$version" ;;
    *) bad "health_contract_version='$version' is not a known contract" ;;
  esac
  config="$(manifest_value config_path)"
  declared_version="$(sed -n 's/^[[:space:]]*contract_version[[:space:]]*:[[:space:]]*//p' "$config" 2>/dev/null | tail -1 | tr -d ' "'"'"'')"
  [ "$declared_version" = "$version" ] \
    && ok "config declares the same contract version ($declared_version)" \
    || bad "config declares contract_version='$declared_version' but the manifest says '$version'"
  unit_version="$(sed -n 's/^[[:space:]]*Environment[[:space:]]*=[[:space:]]*IBKR_MCP_HEALTH__CONTRACT_VERSION[[:space:]]*=[[:space:]]*//p' "$unit" 2>/dev/null | tail -1 | tr -d ' ')"
  [ "$unit_version" = "$version" ] \
    && ok "unit declares the same contract version ($unit_version)" \
    || bad "unit declares IBKR_MCP_HEALTH__CONTRACT_VERSION='$unit_version' but the manifest says '$version'"

  # 5. client identity
  local unit_cid
  unit_cid="$(sed -n 's/^[[:space:]]*Environment[[:space:]]*=[[:space:]]*IBKR_MCP_IBKR__CLIENT_ID[[:space:]]*=[[:space:]]*//p' "$unit" 2>/dev/null | tail -1 | tr -d ' ')"
  [ "$unit_cid" = "$(manifest_value client_id)" ] \
    && ok "unit client id matches the manifest ($unit_cid)" \
    || bad "unit client id '$unit_cid' != manifest client_id '$(manifest_value client_id)'"

  # 6. rollback package materialized with the declared checksums (§6.3 step 4)
  local rdir
  rdir="$(manifest_value rollback_dir)"
  if [ -d "$rdir" ]; then
    [ "$(sha256_of "$(package_bin "$rdir")")" = "$(manifest_value rollback_binary_sha256)" ] \
      && ok "rollback package binary matches rollback_binary_sha256" \
      || bad "rollback package binary does not match rollback_binary_sha256"
    # The unit slot: a FIRST-INSTALL package has none (the tuple's unit never
    # existed here), and the manifest records that absence as 'none' - a value
    # that can never silently stand in for a checksum.
    local rb_unit_sha
    rb_unit_sha="$(sha256_of "$(package_unit "$rdir")" || true)"
    if [ -z "$rb_unit_sha" ]; then
      [ "$(manifest_value rollback_unit_sha256)" = "none" ] \
        && ok "first-install rollback package: no predecessor unit (rollback_unit_sha256=none)" \
        || bad "rollback package has no unit slot but rollback_unit_sha256='$(manifest_value rollback_unit_sha256)' (must declare none)"
    else
      [ "$rb_unit_sha" = "$(manifest_value rollback_unit_sha256)" ] \
        && ok "rollback package unit matches rollback_unit_sha256" \
        || bad "rollback package unit does not match rollback_unit_sha256"
    fi
    [ "$(sha256_of "$(package_config "$rdir")")" = "$(manifest_value rollback_config_sha256)" ] \
      && ok "rollback package config matches rollback_config_sha256" \
      || bad "rollback package config does not match rollback_config_sha256"
  else
    bad "rollback_dir does not exist: ${rdir:-<none>} (run --mode=snapshot-rollback first)"
  fi
}

# --- modes --------------------------------------------------------------------
mode_manifest() {
  # Default to the DEPLOYED binary path: with the binary-as-pacman coexistence
  # (ibkr-mcp-rs package owns /usr/local/bin/ibkr-mcp-rs) the tuple's binary IS
  # the pacman-owned file, and `--mode=verify` compares against exactly this
  # path. A stale `dist/` staging artifact must never be a default (the policy
  # gate rejects superseded-artifact defaults outright).
  local bin="${ARTIFACT_BIN_PATH:-/usr/local/bin/ibkr-mcp-rs}"
  local rdir="${ROLLBACK_DIR_OVERRIDE:-$ROLLBACK_DIR_DEFAULT}"
  [ -f "$bin" ] || die "binary not found: $bin (pass --binary PATH)"
  local config="$REPO_ROOT/config/default.yaml"
  [ -f "$config" ] || die "config not found: $config"
  local version
  version="$(sed -n 's/^[[:space:]]*contract_version[[:space:]]*:[[:space:]]*//p' "$config" | tail -1 | tr -d ' "'"'"'')"
  [ -n "$version" ] || die "config/default.yaml declares no contract_version (plan §6.1)"
  local client_id
  client_id="$(sed -n 's/^[[:space:]]*Environment[[:space:]]*=[[:space:]]*IBKR_MCP_IBKR__CLIENT_ID[[:space:]]*=[[:space:]]*//p' "$SEL_UNIT_SRC" | tail -1 | tr -d ' ')"
  local broker_port
  broker_port="$(sed -n 's/^[[:space:]]*Environment[[:space:]]*=[[:space:]]*IBKR_MCP_IBKR__PORT[[:space:]]*=[[:space:]]*//p' "$SEL_UNIT_SRC" | tail -1 | tr -d ' ')"
  # Per-tuple state paths come from the unit itself when it declares them (the
  # paper unit does - /var/lib/ibkr-mcp-paper/... - the live unit keeps the
  # historical /tmp defaults).
  local persist_path heartbeat_path
  persist_path="$(sed -n 's/^[[:space:]]*Environment[[:space:]]*=[[:space:]]*IBKR_MCP_PERSIST_PATH[[:space:]]*=[[:space:]]*//p' "$SEL_UNIT_SRC" | tail -1 | tr -d ' ')"
  heartbeat_path="$(sed -n 's/^[[:space:]]*Environment[[:space:]]*=[[:space:]]*IBKR_MCP_HEARTBEAT_PATH[[:space:]]*=[[:space:]]*//p' "$SEL_UNIT_SRC" | tail -1 | tr -d ' ')"
  : "${persist_path:=/tmp/ibkr-mcp-last-good.json}"
  : "${heartbeat_path:=/tmp/ibkr-mcp-heartbeat}"
  # Rollback-slot hashes of the captured package. An absent slot declares the
  # sentinel `none` - required keys must be non-empty, no *service file exists
  # for the gate to compare it against, and 'none' can never silently stand in
  # for a checksum (verify_manifest_against_host enforces exactly this).
  local rb_binary_sha rb_unit_sha rb_config_sha park_reason
  # `|| true`: sha256_of is a sha256sum|cut pipeline under pipefail, so a MISSING
  # file exits 1 - and a bare `v=$(...)` assignment then aborts the script under
  # set -e with no message. An absent slot must yield empty (-> `none` below).
  rb_binary_sha="$(sha256_of "$(package_bin "$rdir")" || true)"
  rb_unit_sha="$(sha256_of "$(package_unit "$rdir")" || true)"
  rb_config_sha="$(sha256_of "$(package_config "$rdir")" || true)"
  [ -n "$rb_unit_sha" ]    || rb_unit_sha=none
  [ -n "$rb_binary_sha" ]  || rb_binary_sha=none
  [ -n "$rb_config_sha" ]  || rb_config_sha=none
  park_reason="park is the standing policy of this refactor: a rollback restores the replaced tuple's binary and config but never reinstates a Gateway activation edge or a weaker restart policy (plan 6.3 step 9). It is recorded per deployment, because the tuple being replaced changes even when the policy does not."
  if [ "$rb_unit_sha" = none ]; then
    park_reason="FIRST INSTALL of $SEL_UNIT_NAME: no predecessor unit exists on this host (rollback_unit_sha256=none records the absence). A rollback of this tuple REMOVES the installed unit and restores absence; the shared binary and config are not touched, they belong to the replaced tuple's own rollback package (plan 6.3 step 9)."
  fi
  # section 6.3 step 7: a client id other than the documented service id 204
  # must declare WHERE its migration is recorded. Override the reference text
  # with CLIENT_ID_MIGRATION_REF='<ref>'. An empty line is inert in the manifest.
  local migration_ref_line=""
  if [ "$client_id" != "204" ]; then
    migration_ref_line="client_id_migration_ref=${CLIENT_ID_MIGRATION_REF:-client id $client_id for $SEL_UNIT_NAME is the per-instance id fixed by the 2026-10-11 ibgw-ibc retarget (one client id per gateway instance: live=204, paper=205); see systemd/$SEL_UNIT_NAME and PLAN-paper-unit-owning-installer.md}"
  fi
  cat <<EOF
# Qualified ibkr-mcp artifact tuple, generated at $(date -u +%Y-%m-%dT%H:%M:%SZ)
# by $(basename "${BASH_SOURCE[0]}") --mode=manifest
# Validate with: bash $REPO_ROOT/../ibkr-stack/scripts/check-mcp-policy.sh --manifest <this file> --installed
manifest_version=1
source_commit=$(cd "$REPO_ROOT" && git rev-parse HEAD)
snapshot_tree_sha256=$(snapshot_tree_hash)
cargo_lock_sha256=$(sha256_of "$REPO_ROOT/Cargo.lock")
rustc_version=$(rustc --version 2>/dev/null || echo unknown)
binary_path=$bin
binary_sha256=$(sha256_of "$bin")
unit_path=$SEL_UNIT_SRC
unit_sha256=$(sha256_of "$SEL_UNIT_SRC")
config_path=$config
config_sha256=$(sha256_of "$config")
health_contract_version=$version
client_id=$client_id
$migration_ref_line
broker_endpoint_mode=proxy
broker_port=$broker_port
persist_path=$persist_path
heartbeat_path=$heartbeat_path
owning_installer=$REPO_ROOT/scripts/install-mcp-tuple.sh
rollback_mode=park
rollback_park_reason=$park_reason
rollback_dir=$rdir
rollback_new_generation=${ROLLBACK_NEW_GENERATION:-1}
rollback_previous_generation=${ROLLBACK_PREVIOUS_GENERATION:-0}
rollback_restores_activation_edge=no
rollback_restores_restart_sec=no
rollback_binary_sha256=$rb_binary_sha
rollback_unit_sha256=$rb_unit_sha
rollback_config_sha256=$rb_config_sha
EOF
}

mode_snapshot_rollback() {
  local rdir="${ROLLBACK_DIR_OVERRIDE:-$ROLLBACK_DIR_DEFAULT}"
  local cur_bin="$INSTALL_BIN_DEST" cur_unit="$SEL_UNIT_DEST" cur_config="$REPO_ROOT/config/default.yaml"
  [ -f "$cur_bin" ] || die "no installed binary at $cur_bin"
  [ -f "$cur_config" ] || die "no config at $cur_config"
  local first_install=0
  if [ ! -f "$cur_unit" ]; then
    first_install=1
    step "FIRST INSTALL of $SEL_UNIT_NAME: no installed unit at $cur_unit"
    say "capturing the shared pre-deploy binary and config only - there is no"
    say "predecessor unit to preserve. The manifest will declare rollback_unit_sha256=none."
  fi
  if [ -d "$rdir" ] && [ -n "$(ls -A "$rdir" 2>/dev/null || true)" ] && [ "$FORCE" -ne 1 ]; then
    die "rollback package $rdir is not empty; re-run with --force to overwrite"
  fi
  if [ "$first_install" -eq 0 ] && [ "$(id -u)" -ne 0 ] && [ ! -r "$cur_unit" ]; then
    step "re-executing under sudo to read the installed unit"
    exec sudo -- "$0" "$@" --force
  fi
  step "materializing the pre-install rollback package into $rdir"
  if [ "$first_install" -eq 1 ]; then
    write_package "$rdir" "$cur_bin" "" "$cur_config"
  else
    write_package "$rdir" "$cur_bin" "$cur_unit" "$cur_config"
  fi
  ls -l "$rdir"
  package_hashes "$rdir"
  say ""
  # Say what is actually true of THIS package: the park rationale is about the
  # activation edge, and a package whose unit has none (a redeployment after the
  # edge was removed) must not be described as if it had one.
  if [ "$first_install" -eq 1 ]; then
    say "This is a FIRST INSTALL. The package records the shared pre-deploy artifacts;"
    say "the unit slot is absent and rollback_unit_sha256=none. A rollback of this"
    say "tuple removes the installed unit and restores absence (park policy: no"
    say "predecessor is ever installed, no activation edge, no weaker restart policy)."
  elif grep -qE '^[[:space:]]*(Requires|Wants|BindsTo|PartOf|Requisite|Upholds)[[:space:]]*=.*(gateway|ibgw-ibc)' "$cur_unit"; then
    say "This is the tuple being REPLACED. Its unit CARRIES the Gateway activation"
    say "edge, so rollback_mode=park applies: a rollback restores its binary/config"
    say "only, never the unit."
  else
    say "This is the tuple being REPLACED. Its unit carries NO Gateway activation"
    say "edge, so a rollback may restore all three artifacts - but it still never"
    say "reinstates an activation edge or a weaker restart policy (park policy)."
  fi
}

mode_install() {
  [ -n "$MANIFEST" ] || die "--mode=install needs --manifest FILE"
  require_root "$@"
  step "verifying the manifest before any write"
  verify_manifest_against_host
  [ "$FAILS" -eq 0 ] || die "tuple verification failed ($FAILS FAILs) - nothing installed"

  local unit_dest_src="$SEL_UNIT_SRC" bin_src config_src
  bin_src="$(manifest_value binary_path)"
  config_src="$(manifest_value config_path)"

  local stamp; stamp="$(date +%Y%m%d-%H%M%S)"
  local dated="$REPO_ROOT/backups/$stamp-pre-install"
  local cur_unit_slot="$SEL_UNIT_DEST"
  [ -f "$SEL_UNIT_DEST" ] || cur_unit_slot=
  step "dated backup of what is being replaced -> $dated"
  write_package "$dated" "$INSTALL_BIN_DEST" "$cur_unit_slot" "$config_src"

  step "forward package (what a rollback would return to) -> $FORWARD_DIR_DEFAULT"
  write_package "$FORWARD_DIR_DEFAULT" "$bin_src" "$unit_dest_src" "$config_src"

  step "installing atomically (unit first, then binary; the running process is untouched)"
  # Unit first: new unit + old binary is a bounded-restart combination, whereas
  # old unit + new binary would carry the activation edge into the next start.
  # Each unit's install keeps its own literal words (ownership-analyzer note).
  if [ "$UNIT_SEL" = paper ]; then
    install -m 0644 -o root -g root "$PAPER_UNIT_SRC" "/etc/systemd/system/.$PAPER_UNIT_NAME.new"
    mv -f "/etc/systemd/system/.$PAPER_UNIT_NAME.new" "$PAPER_UNIT_DEST"
  else
    install -m 0644 -o root -g root "$UNIT_SRC" "/etc/systemd/system/.$UNIT_NAME.new"
    mv -f "/etc/systemd/system/.$UNIT_NAME.new" "$UNIT_DEST"
  fi
  say "  unit     -> $SEL_UNIT_DEST"
  if [ -n "${SEL_DROPIN_DEST:-}" ] && [ -f "$SEL_DROPIN_DEST" ]; then
    say "  removing superseded drop-in $SEL_DROPIN_DEST"
    rm -f "$SEL_DROPIN_DEST"
    rmdir "$DROPIN_DEST_DIR" 2>/dev/null || true
  fi
  install -m 0755 -o root -g root "$bin_src" "/usr/local/bin/.ibkr-mcp-rs.new"
  mv -f "/usr/local/bin/.ibkr-mcp-rs.new" "$INSTALL_BIN_DEST"
  say "  binary   -> $INSTALL_BIN_DEST"
  say "  config   -> $config_src (deployed in place; hash verified above and below)"

  systemctl daemon-reload
  systemctl enable "$SEL_UNIT_NAME" >/dev/null 2>&1 || true

  step "post-install artifact check"
  [ "$(sha256_of "$SEL_UNIT_DEST")" = "$(manifest_value unit_sha256)" ] \
    && ok "installed unit matches unit_sha256" || bad "installed unit does not match unit_sha256"
  [ "$(sha256_of "$INSTALL_BIN_DEST")" = "$(manifest_value binary_sha256)" ] \
    && ok "installed binary matches binary_sha256" || bad "installed binary does not match binary_sha256"
  [ "$(sha256_of "$config_src")" = "$(manifest_value config_sha256)" ] \
    && ok "config still matches config_sha256" || bad "config no longer matches config_sha256"

  step "effective systemd graph (plan §6.1: no Gateway activation edge)"
  systemctl show "$SEL_UNIT_NAME" -p Requires -p Wants -p After -p FragmentPath -p DropInPaths
  if systemctl show "$SEL_UNIT_NAME" -p Requires -p Wants | grep -qE 'gateway|ibgw-ibc'; then
    bad "EFFECTIVE graph still activates a Gateway unit"
  else
    ok "effective Requires=/Wants= carry no Gateway unit"
  fi
  if command -v systemd-analyze >/dev/null 2>&1; then
    systemd-analyze verify "$SEL_UNIT_NAME" >/dev/null 2>&1 \
      && ok "systemd-analyze verify accepts $SEL_UNIT_NAME" \
      || warn "systemd-analyze verify reported issues for $SEL_UNIT_NAME"
  fi
  [ "$FAILS" -eq 0 ] || die "post-install checks failed ($FAILS FAILs)"
  say ""
  say "Installed. The RUNNING process is untouched: no start, stop or restart was"
  say "issued. Restarting $SEL_UNIT_NAME is a separate, explicitly approved action"
  say "(plan §6.3 step 5)."
}

mode_unit() {
  [ -n "$MANIFEST" ] && { verify_manifest_against_host; [ "$FAILS" -eq 0 ] || die "tuple verification failed"; }
  require_root "$@"
  step "installing the unit only ($SEL_UNIT_NAME)"
  # Each unit's install keeps its own literal words (ownership-analyzer note).
  if [ "$UNIT_SEL" = paper ]; then
    install -m 0644 -o root -g root "$PAPER_UNIT_SRC" "/etc/systemd/system/.$PAPER_UNIT_NAME.new"
    mv -f "/etc/systemd/system/.$PAPER_UNIT_NAME.new" "$PAPER_UNIT_DEST"
  else
    install -m 0644 -o root -g root "$UNIT_SRC" "/etc/systemd/system/.$UNIT_NAME.new"
    mv -f "/etc/systemd/system/.$UNIT_NAME.new" "$UNIT_DEST"
  fi
  systemctl daemon-reload
  systemctl show "$SEL_UNIT_NAME" -p Requires -p Wants -p After -p FragmentPath
  if systemctl show "$SEL_UNIT_NAME" -p Requires -p Wants | grep -qE 'gateway|ibgw-ibc'; then
    bad "EFFECTIVE graph still activates a Gateway unit"
  else
    ok "effective Requires=/Wants= carry no Gateway unit"
  fi
  [ "$FAILS" -eq 0 ] || die "post-install checks failed"
  say "done. Takes effect on the next approved start/restart of $SEL_UNIT_NAME."
}

mode_dropin() {
  require_root "$@"
  if [ -z "${SEL_DROPIN_SRC:-}" ]; then
    step "restart-policy drop-in for $SEL_UNIT_NAME"
    say "nothing to install: $SEL_UNIT_NAME carries its restart policy INLINE in the"
    say "unit file (Restart=always, RestartSec=30, StartLimitIntervalSec=300,"
    say "StartLimitBurst=5) - the same contract as the live unit's drop-in."
    return 0
  fi
  [ -f "$SEL_DROPIN_SRC" ] || die "missing $SEL_DROPIN_SRC"
  step "installing the restart-policy drop-in only"
  install -d -m 0755 "$DROPIN_DEST_DIR"
  install -m 0644 -o root -g root "$DROPIN_SRC" "$DROPIN_DEST"
  systemctl daemon-reload
  say "done. Note: a drop-in cannot remove the Requires= edge; use --mode=unit."
}

mode_verify() {
  local rdir; rdir="$(manifest_value rollback_dir)"
  step "artifacts vs the manifest"
  if [ -n "$MANIFEST" ]; then
    local path declared actual
    for pair in "binary_path:binary_sha256" "unit_path:unit_sha256" "config_path:config_sha256"; do
      path="$(manifest_value "${pair%%:*}")"; declared="$(manifest_value "${pair##*:}")"
      [ -f "$path" ] || { bad "${pair%%:*} missing: $path"; continue; }
      actual="$(sha256_of "$path")"
      [ "$actual" = "$declared" ] && ok "$(basename "$path") = $actual" || bad "$(basename "$path") = $actual (manifest $declared)"
    done
    [ "$(sha256_of "$INSTALL_BIN_DEST")" = "$(manifest_value binary_sha256)" ] \
      && ok "installed $INSTALL_BIN_DEST matches the manifest binary" \
      || bad "installed $INSTALL_BIN_DEST does not match the manifest binary"
    [ "$(sha256_of "$SEL_UNIT_DEST")" = "$(manifest_value unit_sha256)" ] \
      && ok "installed $SEL_UNIT_DEST matches the manifest unit" \
      || bad "installed $SEL_UNIT_DEST does not match the manifest unit"
  else
    warn "no manifest given: reporting installed state only"
    say "  $INSTALL_BIN_DEST sha256 $(sha256_of "$INSTALL_BIN_DEST")"
    say "  $SEL_UNIT_DEST sha256 $(sha256_of "$SEL_UNIT_DEST")"
    say "  config sha256 $(sha256_of "$REPO_ROOT/config/default.yaml")"
  fi

  step "effective systemd graph"
  systemctl show "$SEL_UNIT_NAME" -p Requires -p Wants -p After -p FragmentPath -p DropInPaths -p ExecMainStartTimestamp -p InvocationID -p NRestarts
  if systemctl show "$SEL_UNIT_NAME" -p Requires -p Wants | grep -qE 'gateway|ibgw-ibc'; then
    bad "EFFECTIVE graph activates a Gateway unit"
  else
    ok "effective Requires=/Wants= carry no Gateway unit"
  fi

  step "running process identity"
  local main_pid
  main_pid="$(systemctl show "$SEL_UNIT_NAME" -p MainPID --value || true)"
  if [ -n "$main_pid" ] && [ "$main_pid" != "0" ]; then
    say "  MainPID=$main_pid"
    ls -l "/proc/$main_pid/exe" 2>/dev/null || true
    say "  exe sha256 $(sha256_of "/proc/$main_pid/exe" 2>/dev/null || echo unknown)"
    if [ -n "$MANIFEST" ]; then
      [ "$(sha256_of "/proc/$main_pid/exe" 2>/dev/null || echo unknown)" = "$(manifest_value binary_sha256)" ] \
        && ok "the RUNNING process is the manifest binary" \
        || bad "the RUNNING process is not the manifest binary (still the old one?)"
    fi
  else
    warn "unit has no running process"
  fi

  step "Gateway invocation identity (must be unchanged across an MCP deploy)"
  systemctl show $GATEWAY_UNITS -p InvocationID -p ActiveEnterTimestamp -p MainPID -p NRestarts

  step "health endpoints"
  local url="${HEALTH_URL:-$HEALTH_URL_DEFAULT}"
  local base="${url%/health/live}"
  local body code
  body="$(tmp_file health)"
  for ep in health/live health/ready health/broker version; do
    printf '  %-13s ' "$ep"
    : >"$body"
    if code="$(curl -s -o "$body" -w '%{http_code}' --max-time 5 "$base/$ep")"; then
      printf '%s ' "$code"
      tr -d '\n' <"$body" | cut -c1-200
    else
      # The fetch failed: report ERR and NOTHING else. Printing the body here is
      # what produced stale rows in the Phase 1 evidence.
      printf 'ERR (no response)'
    fi
    say ""
  done
  local reported
  # `|| true`: under pipefail a dead endpoint makes this bare assignment abort
  # the whole verify under set -e - evidence collection must survive a down bridge.
  reported="$(curl -s --max-time 5 "$base/version" | sed -n 's/.*"contractVersion":"\([^"]*\)".*/\1/p' || true)"
  if [ -n "$MANIFEST" ]; then
    [ "$reported" = "$(manifest_value health_contract_version)" ] \
      && ok "running process reports contractVersion=$reported" \
      || bad "running process reports contractVersion='$reported', manifest says '$(manifest_value health_contract_version)'"
  fi
  say ""
  say "rollback package: ${rdir:-<none declared>}"
}

mode_rollback() {
  [ "$APPROVED" -eq 1 ] || die "--mode=rollback requires --approved (plan §6.3 step 9: every rollback needs renewed operator approval)"
  require_root "$@"
  local dir
  case "$RESTORE" in
    package) dir="${FROM_DIR:-${ROLLBACK_DIR_OVERRIDE:-$ROLLBACK_DIR_DEFAULT}}" ;;
    forward) dir="${FROM_DIR:-$FORWARD_DIR_DEFAULT}" ;;
  esac
  [ -d "$dir" ] || die "package directory not found: $dir"
  local pkg_bin pkg_unit pkg_config pkg_shape
  pkg_bin="$(package_bin "$dir")"; pkg_unit="$(package_unit "$dir")"; pkg_config="$(package_config "$dir")"
  pkg_shape=triple
  [ -f "$pkg_unit" ] || pkg_shape=first-install
  if [ "$pkg_shape" = triple ]; then
    [ -f "$pkg_bin" ] && [ -f "$pkg_unit" ] && [ -f "$pkg_config" ] \
      || die "package $dir is incomplete (need $(basename "$pkg_bin"), $(basename "$pkg_unit"), $(basename "$pkg_config"))"
  else
    [ -f "$pkg_bin" ] && [ -f "$pkg_config" ] \
      || die "first-install package $dir is incomplete (need $(basename "$pkg_bin"), $(basename "$pkg_config"))"
  fi

  step "rollback source: $dir ($RESTORE, $pkg_shape)"
  ls -l "$dir"
  package_hashes "$dir"

  # §6.3 step 9: a rollback must preserve the safety invariants active at this
  # stage. The pre-install unit carries the Gateway activation edge, so it is
  # parked, never restored. A FIRST-INSTALL package has no predecessor unit at
  # all: the rollback removes the installed unit and restores absence - the
  # shared binary/config belong to the replaced tuple's own rollback package.
  local restore_unit=1
  if [ "$pkg_shape" = first-install ]; then
    restore_unit=0
    warn "FIRST INSTALL: no predecessor unit exists - $SEL_UNIT_NAME is REMOVED (absence restored)"
    warn "the shared binary and config are NOT touched (they belong to the replaced tuple's rollback)"
  elif grep -qE '^[[:space:]]*(Requires|Wants|BindsTo|PartOf|Requisite|Upholds)[[:space:]]*=.*gateway' "$pkg_unit"; then
    restore_unit=0
    warn "PARK: $pkg_unit carries a Gateway activation edge - the unit is NOT restored"
    warn "PARK: restoring it would violate the Phase-1 no-Gateway-activation invariant"
  fi
  if [ "$RESTORE" = "package" ] && [ "$restore_unit" -eq 0 ] && [ -n "$MANIFEST" ]; then
    [ "$(manifest_value rollback_restores_activation_edge)" = "no" ] \
      && ok "manifest agrees: rollback_restores_activation_edge=no" \
      || bad "manifest says rollback_restores_activation_edge=$(manifest_value rollback_restores_activation_edge) but the package unit has the edge"
  fi

  step "gateway identity BEFORE (must be identical after)"
  systemctl show $GATEWAY_UNITS -p InvocationID -p ActiveEnterTimestamp -p MainPID

  step "stopping $SEL_UNIT_NAME (MCP only)"
  systemctl stop "$SEL_UNIT_NAME" || true
  say "  stopped at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

  step "restoring the package artifacts"
  if [ "$pkg_shape" = first-install ]; then
    rm -f "$SEL_UNIT_DEST"
    [ -n "${SEL_DROPIN_DEST:-}" ] && rm -f "$SEL_DROPIN_DEST"
    say "  unit     -> REMOVED (absence restored; nothing shared was touched)"
  else
  install -m 0755 -o root -g root "$pkg_bin" "/usr/local/bin/.ibkr-mcp-rs.new"
  mv -f "/usr/local/bin/.ibkr-mcp-rs.new" "$INSTALL_BIN_DEST"
  say "  binary   -> $INSTALL_BIN_DEST ($(sha256_of "$INSTALL_BIN_DEST"))"
  if [ "$restore_unit" -eq 1 ]; then
    # Each unit's restore keeps its own literal words (ownership-analyzer note).
    if [ "$UNIT_SEL" = paper ]; then
      install -m 0644 -o root -g root "$pkg_unit" "/etc/systemd/system/.$PAPER_UNIT_NAME.new"
      mv -f "/etc/systemd/system/.$PAPER_UNIT_NAME.new" "$PAPER_UNIT_DEST"
    else
      install -m 0644 -o root -g root "$pkg_unit" "/etc/systemd/system/.$UNIT_NAME.new"
      mv -f "/etc/systemd/system/.$UNIT_NAME.new" "$UNIT_DEST"
    fi
    say "  unit     -> $SEL_UNIT_DEST ($(sha256_of "$SEL_UNIT_DEST"))"
  else
    say "  unit     -> PARKED (kept: $(sha256_of "$SEL_UNIT_DEST"))"
  fi
  cp -a "$pkg_config" "$REPO_ROOT/config/default.yaml"
  say "  config   -> $REPO_ROOT/config/default.yaml ($(sha256_of "$REPO_ROOT/config/default.yaml"))"
  fi
  systemctl daemon-reload

  if [ "$pkg_shape" = first-install ]; then
    step "nothing to start"
    say "$SEL_UNIT_NAME was removed by this rollback; no start is issued."
  else
  step "starting $SEL_UNIT_NAME (MCP only, never the Gateway)"
  systemctl start "$SEL_UNIT_NAME"
  say "  started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local base="${HEALTH_URL:-$HEALTH_URL_DEFAULT}"; base="${base%/health/live}"
  local ready=0
  for _ in $(seq 1 30); do
    if curl -fsS --max-time 3 "$base/health/live" >/dev/null 2>&1; then ready=1; break; fi
    sleep 1
  done
  [ "$ready" -eq 1 ] && ok "$base/health/live answered 200" || bad "$base/health/live did not answer within 30s"
  say "  /health/live  $(curl -s --max-time 5 "$base/health/live" || echo ERR)"
  say "  /version      $(curl -s --max-time 5 "$base/version" || echo ERR)"
  systemctl show "$SEL_UNIT_NAME" -p Requires -p Wants -p InvocationID -p NRestarts
  fi

  step "gateway identity AFTER (must equal BEFORE)"
  systemctl show $GATEWAY_UNITS -p InvocationID -p ActiveEnterTimestamp -p MainPID

  [ "$FAILS" -eq 0 ] || die "rollback verification failed ($FAILS FAILs)"
  say ""
  say "Rollback to '$RESTORE' complete. A rollback allocates a new generation:"
  say "record it, and never re-activate a previous generation (§6.3 step 9)."
}

case "$MODE" in
  check)
    [ -n "$MANIFEST" ] || die "--mode=check needs --manifest FILE"
    step "artifact tuple (manifest)"
    verify_manifest_against_host
    say ""
    if [ "$FAILS" -eq 0 ]; then say "RESULT: consistent (0 FAILs)"; exit 0; fi
    say "RESULT: $FAILS FAIL(s)"; exit 1 ;;
  manifest)         mode_manifest ;;
  snapshot-rollback) mode_snapshot_rollback "$@" ;;
  install)          mode_install "$@" ;;
  unit)             mode_unit "$@" ;;
  dropin)           mode_dropin "$@" ;;
  verify)           mode_verify ;;
  rollback)         mode_rollback "$@" ;;
  *) die "unknown mode: $MODE" ;;
esac
