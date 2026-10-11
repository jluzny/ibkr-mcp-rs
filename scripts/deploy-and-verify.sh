#!/usr/bin/env bash
# RETIRED 2026-09-22 (PLAN-gateway-supervisor-refactor.md §6.3, §13).
#
# This script used to check a staged build against a hard-pinned sha256 and then
# deploy + restart in one step. The pin is the problem: it froze the tuple at a
# superseded build (Phase 0 finding F5), so the script would have refused the
# qualified artifact and accepted only the old one.
#
# The tuple is now identified by an artifact manifest (source snapshot, lock
# file, toolchain, binary/unit/config checksums, contract version, rollback
# package) and validated by the pre-install gate before anything is written:
#
#   bash ../ibkr-stack/scripts/check-mcp-policy.sh --manifest <tuple> --installed
#   sudo bash scripts/install-mcp-tuple.sh --mode=install --manifest <tuple>
#   bash scripts/install-mcp-tuple.sh --mode=verify    --manifest <tuple>
#
# Nothing was installed, copied or restarted by this invocation.
set -Eeuo pipefail

cat >&2 <<'EOF'
!! deploy-and-verify.sh is RETIRED - its hard-pinned sha256 froze the deployment
   tuple at a superseded build.

   Use instead (from the repository root):
     bash scripts/install-mcp-tuple.sh --mode=check   --manifest <tuple.properties>
     sudo bash scripts/install-mcp-tuple.sh --mode=install --manifest <tuple.properties>
     bash scripts/install-mcp-tuple.sh --mode=verify  --manifest <tuple.properties>
     IBKR_MCP_URL=http://127.0.0.1:8881/mcp python3 scripts/verify-market-data-tools.py

   Nothing was installed, copied or restarted by this invocation.
EOF
exit 2
