#!/usr/bin/env python3
"""Exercise the ibkr-mcp-rs tool surface over streamable HTTP and dump the output.

Usage:
    IBKR_MCP_URL=http://127.0.0.1:8899/mcp python3 scripts/verify-market-data-tools.py [--json out.json]

Calls every tool touched (or added) by the historical-bars / IV-rank / scanner /
WSH-events work, plus the pre-existing tools as a regression check. Prints each
result verbatim so the output can be pasted into a deploy note.
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

# (label, tool, args)
CALLS = [
    ("regression: connection", "get_connection_status", {}),
    ("new: historical IV bars", "get_historical_data", {
        "symbol": "WOLF",
        "duration": "1 Y",
        "bar_size": "1 day",
        "what_to_show": "OPTION_IMPLIED_VOLATILITY",
    }),
    ("new: historical TRADES bars", "get_historical_data", {
        "symbol": "WOLF",
        "duration": "1 M",
        "bar_size": "1 day",
        "what_to_show": "TRADES",
    }),
    ("new: IV rank", "get_iv_rank", {"symbol": "WOLF"}),
    ("new: IV rank (explicit window)", "get_iv_rank", {"symbol": "SOFI", "window": "252"}),
    ("new: scanner", "run_scanner", {"scan_code": "HOT_BY_OPT_VOLUME", "rows": 10}),
    ("new: scanner NATCOMB (multi-leg)", "run_scanner", {
        "scan_code": "COMBO_LATEST_TRADE",
        "instrument": "NATCOMB",
        "location": "NATCOMB.OPT.US",
        "rows": 5,
        # underConID must be the UNDERLYING's conid (WOLF = 817815740).
        # A leg conid (e.g. 910669144) silently returns 0 rows.
        "tag_filters": {"underConID": "817815740"},
    }),
    ("new: WSH events", "get_events", {"symbol": "WOLF"}),
    ("new: historical ticks", "get_historical_ticks", {
        "symbol": "AAPL",
        "number_of_ticks": 10,
        "tick_type": "TRADES",
        "start_datetime": "2026-09-15 13:30:00",
        "end_datetime": "2026-09-15 20:00:00",
    }),
    ("regression: account info", "get_account_info", {}),
    ("regression: positions", "get_positions", {}),
    ("regression: option quote", "get_option_quote", {
        "symbol": "WOLF", "expiration": "20261120", "strike": 25, "right": "C",
    }),
    ("regression: bulk quotes", "get_bulk_quotes", {"symbols": ["WOLF", "SOFI"]}),
    ("regression: option chain", "get_option_chain", {"symbol": "WOLF"}),
]


async def run(url, only=None):
    results = []
    async with streamable_http_client(url) as conn:
        # mcp >= 1.10 yields (read, write); older builds yield (read, write, get_id)
        read, write = conn[0], conn[1]
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print(f"== tools exposed ({len(tools.tools)}): "
                  f"{', '.join(sorted(t.name for t in tools.tools))}\n")

            for label, tool, args in CALLS:
                if only and tool not in only:
                    continue
                started = time.time()
                try:
                    result = await session.call_tool(tool, args)
                    text = "\n".join(
                        c.text for c in result.content if getattr(c, "text", None)
                    )
                    is_error = bool(getattr(result, "isError", False))
                except Exception as exc:  # noqa: BLE001 — report, don't crash the sweep
                    text = f"CLIENT EXCEPTION: {exc!r}"
                    is_error = True
                elapsed = time.time() - started
                results.append({
                    "label": label,
                    "tool": tool,
                    "args": args,
                    "is_error": is_error,
                    "elapsed_s": round(elapsed, 2),
                    "text": text,
                })

                print(f"----- {label} :: {tool} ({elapsed:.1f}s, error={is_error})")
                print(text if len(text) <= 6000 else text[:6000] + "\n...[truncated]...")
                print()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--json", default=None, help="write full results as JSON here")
    parser.add_argument("--only", nargs="*", default=None, help="tool names to call")
    opts = parser.parse_args()

    results = asyncio.run(run(opts.url, opts.only))

    if opts.json:
        with open(opts.json, "w") as fh:
            json.dump(results, fh, indent=2)
        print(f"wrote {opts.json}")

    failures = [r for r in results if r["is_error"]]
    if failures:
        print(f"!! {len(failures)} call(s) reported an error: "
              f"{', '.join(r['tool'] for r in failures)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
