from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
import structlog
from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client

logger = structlog.get_logger()


def _parse_tool_result(result: Any) -> str:
    """
    Normalize MCP CallToolResult into a JSON string for the existing agent layer.
    Prefers structuredContent when available.
    """
    if hasattr(result, "structuredContent") and result.structuredContent:
        return json.dumps(result.structuredContent, indent=2, default=str)

    texts: list[str] = []
    content = getattr(result, "content", []) or []
    for item in content:
        if isinstance(item, types.TextContent):
            texts.append(item.text)
        elif isinstance(item, types.EmbeddedResource):
            resource = item.resource
            if isinstance(resource, types.TextResourceContents):
                texts.append(resource.text)
        else:
            texts.append(str(item))

    if getattr(result, "isError", False):
        raise RuntimeError("\n".join(texts) if texts else "MCP tool execution failed")

    if len(texts) == 1:
        return texts[0]

    return json.dumps({"content": texts}, indent=2, default=str)


class MCPClient:
    """
    Real MCP client for stdio or Streamable HTTP transports.
    """

    def __init__(
        self,
        transport: str | None = None,
        server_url: str | None = None,
        command: str | None = None,
        args: list[str] | None = None,
    ) -> None:
        self.transport = (transport or os.getenv("MCP_CLIENT_TRANSPORT") or "streamable-http").lower()
        self.server_url = server_url or os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000/mcp")
        self.command = command or os.getenv("MCP_SERVER_COMMAND", "python")
        self.args = args or self._default_stdio_args()

    def _default_stdio_args(self) -> list[str]:
        configured = os.getenv("MCP_SERVER_ARGS")
        if configured:
            return configured.split()

        return ["scripts/run_mcp_server.py"]

    @asynccontextmanager
    async def session(self) -> AsyncIterator[ClientSession]:
        if self.transport == "stdio":
            env = os.environ.copy()
            env["MCP_TRANSPORT"] = "stdio"
            params = StdioServerParameters(
                command=self.command,
                args=self.args,
                env=env,
                cwd=Path(__file__).resolve().parents[2],
            )
            async with stdio_client(params) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    yield session
            return

        if self.transport == "streamable-http":
            async with httpx.AsyncClient(timeout=60.0) as http_client:
                async with streamable_http_client(self.server_url, http_client=http_client) as (
                    read_stream,
                    write_stream,
                    _,
                ):
                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        yield session
            return

        raise ValueError(f"Unsupported MCP transport: {self.transport}")

    async def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> str:
        logger.info(
            "mcp_client_call_tool",
            transport=self.transport,
            tool=tool_name,
            arguments=arguments,
        )
        async with self.session() as session:
            result = await session.call_tool(tool_name, arguments=arguments)
            return _parse_tool_result(result)

    async def list_tools(self) -> list[str]:
        async with self.session() as session:
            result = await session.list_tools()
            return [tool.name for tool in result.tools]

    async def list_resources(self) -> list[str]:
        async with self.session() as session:
            result = await session.list_resources()
            return [str(resource.uri) for resource in result.resources]
