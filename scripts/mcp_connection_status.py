#!/usr/bin/env python3
"""Call the `get_connection_status` MCP tool and print the response + latency.

The cheap "is the broker up?" probe for cron jobs and deploy checks: it must
answer promptly with `brokerConnected: false` while the broker is down, rather
than hanging or erroring (see docs/broker-outage-policy.md).

Usage:
    IBKR_MCP_URL=http://127.0.0.1:8881/mcp python3 scripts/mcp_connection_status.py
    python3 scripts/mcp_connection_status.py --url http://127.0.0.1:8899/mcp --json out.json

Exit codes: 0 = answered, 1 = client error / no answer, 2 = answer arrived but
`brokerConnected` was not a boolean.
"""
import argparse
import asyncio
import json
import os
import sys
import time

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

DEFAULT_URL = os.environ.get("IBKR_MCP_URL", "http://127.0.0.1:8881/mcp")


async def call(url: str, timeout_s: float):
    started = time.time()
    async with streamable_http_client(url) as conn:
        read, write = conn[0], conn[1]
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("get_connection_status", {})
    elapsed = time.time() - started
    text = "\n".join(c.text for c in result.content if getattr(c, "text", None))
    return text, bool(getattr(result, "isError", False)), elapsed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--json", default=None, help="write {text, is_error, elapsed_s} here")
    opts = parser.parse_args()

    try:
        text, is_error, elapsed = asyncio.run(call(opts.url, 30))
    except Exception as exc:  # noqa: BLE001 — report, don't traceback
        print(f"CLIENT EXCEPTION after the tool call: {exc!r}", file=sys.stderr)
        return 1

    print(f"elapsed_s={elapsed:.2f} is_error={is_error}")
    print(text)

    if opts.json:
        with open(opts.json, "w") as fh:
            json.dump({"url": opts.url, "text": text, "is_error": is_error,
                       "elapsed_s": round(elapsed, 3)}, fh, indent=2)

    if is_error:
        return 1
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        print("!! response was not JSON", file=sys.stderr)
        return 2
    if not isinstance(payload.get("brokerConnected"), bool):
        print("!! brokerConnected missing or not a boolean", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
