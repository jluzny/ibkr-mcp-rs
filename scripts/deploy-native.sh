#!/usr/bin/env bash
# RETIRED 2026-09-22 (PLAN-gateway-supervisor-refactor.md §6.3, §13).
#
# This script used to install a staged build over the service binary and then
# `systemctl restart` the unit in the same breath. Both halves are gone:
#
#   * it defaulted to `dist/<name>` and pinned a superseded sha256, so it would
#     have overwritten the qualified artifact with a build nobody qualified
#     (Phase 0 finding F5);
#   * an uncontrolled restart drops the live IBKR session and re-handshakes
#     client id 204, so a restart must be a separate, explicitly approved
#     MCP-only action (plan §6.3 step 5), never a side effect of a copy.
#
# The ONE qualified deployment path is now:
#
#   bash scripts/install-mcp-tuple.sh --mode=check     --manifest <tuple>
#   sudo bash scripts/install-mcp-tuple.sh --mode=install --manifest <tuple>
#   bash scripts/install-mcp-tuple.sh --mode=verify    --manifest <tuple>
#
# and the approved restart is issued by hand, once, inside the approved window.
# See docs/broker-outage-policy.md and README.md.
set -Eeuo pipefail

cat >&2 <<'EOF'
!! deploy-native.sh is RETIRED - it installed a staged build and restarted the
   service in one step, which the qualified deployment path no longer allows.

   Use instead (from the repository root):
     bash scripts/install-mcp-tuple.sh --mode=check   --manifest <tuple.properties>
     sudo bash scripts/install-mcp-tuple.sh --mode=install --manifest <tuple.properties>
     bash scripts/install-mcp-tuple.sh --mode=verify  --manifest <tuple.properties>

   Nothing was installed, copied or restarted by this invocation.
EOF
exit 2
