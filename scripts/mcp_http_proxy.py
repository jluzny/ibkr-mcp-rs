#!/usr/bin/env python3
"""
Stdio-to-HTTP MCP proxy.
Connects to an HTTP MCP server and re-exposes it as a stdio server.
Workaround for hermes-agent's streamable HTTP TaskGroup bug.
"""
import asyncio
import os
import sys

from mcp.server.stdio import stdio_server
from mcp import ClientSession, ServerSession, types
from mcp.client.streamable_http import streamable_http_client

TARGET_URL = os.environ.get("MCP_TARGET_URL", "http://ibkr-mcp:8881/mcp")


async def main():
    # Connect to upstream HTTP MCP server
    async with streamable_http_client(TARGET_URL) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as upstream:
            await upstream.initialize()

            # Discover upstream tools
            tool_result = await upstream.list_tools()
            upstream_tools = {t.name: t for t in tool_result.tools}
            print(f"Proxied {len(upstream_tools)} tools from {TARGET_URL}", file=sys.stderr)

            # Create stdio server that forwards to upstream
            server = ServerSession(
                read_stream=types.JSONRPCMessage,  # placeholder
                write_stream=types.JSONRPCMessage,  # placeholder
                init_options=types.InitializeRequestParams(
                    protocolVersion="2024-11-05",
                    capabilities=types.ServerCapabilities(tools=types.ToolsCapability()),
                    serverInfo=types.Implementation(name="ibkr-mcp-proxy", version="0.1.0"),
                ),
            )

            # Register upstream tools as server tools on the stdio server
            from mcp.server.fastmcp import FastMCP

            app = FastMCP("ibkr-mcp-proxy")

            for name, tool in upstream_tools.items():
                # Create a closure capturing the tool name
                def make_handler(tool_name):
                    async def handler(**kwargs):
                        result = await upstream.call_tool(tool_name, kwargs or {})
                        return [c.model_dump() for c in result.content]

                    return handler

                handler = make_handler(name)
                handler.__name__ = name
                app.tool(name=name, description=tool.description or f"Proxy: {name}")(
                    handler
                )

            # Run as stdio server
            async with stdio_server() as (read, write):
                await app._mcp_server.run(
                    read_stream=read,
                    write_stream=write,
                    initialization_options=app.settings,
                )


if __name__ == "__main__":
    asyncio.run(main())
