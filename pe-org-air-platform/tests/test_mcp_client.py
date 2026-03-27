from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
import sys
import types

import httpx
import pytest


def _load_client_module(monkeypatch):
    structlog_module = types.ModuleType("structlog")

    class _Logger:
        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

    structlog_module.get_logger = lambda: _Logger()
    monkeypatch.setitem(sys.modules, "structlog", structlog_module)

    mcp_module = types.ModuleType("mcp")

    class ClientSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def initialize(self):
            return None

    class StdioServerParameters:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    mcp_module.ClientSession = ClientSession
    mcp_module.StdioServerParameters = StdioServerParameters
    mcp_module.types = types.SimpleNamespace(
        TextContent=type("TextContent", (), {}),
        EmbeddedResource=type("EmbeddedResource", (), {}),
        TextResourceContents=type("TextResourceContents", (), {}),
    )
    monkeypatch.setitem(sys.modules, "mcp", mcp_module)

    stdio_module = types.ModuleType("mcp.client.stdio")
    streamable_http_module = types.ModuleType("mcp.client.streamable_http")

    @asynccontextmanager
    async def _unused_context_manager(*args, **kwargs):
        yield object(), object()

    @asynccontextmanager
    async def _unused_http_context_manager(*args, **kwargs):
        yield object(), object(), object()

    stdio_module.stdio_client = _unused_context_manager
    streamable_http_module.streamable_http_client = _unused_http_context_manager

    monkeypatch.setitem(sys.modules, "mcp.client", types.ModuleType("mcp.client"))
    monkeypatch.setitem(sys.modules, "mcp.client.stdio", stdio_module)
    monkeypatch.setitem(sys.modules, "mcp.client.streamable_http", streamable_http_module)

    sys.modules.pop("app.mcp.client", None)
    import app.mcp.client as client_module

    return importlib.reload(client_module)


@pytest.mark.anyio
async def test_client_falls_back_to_stdio_when_default_http_server_is_unreachable(monkeypatch):
    client_module = _load_client_module(monkeypatch)
    MCPClient = client_module.MCPClient
    client = MCPClient()
    used_transports: list[str] = []

    @asynccontextmanager
    async def fake_stdio_session(self):
        used_transports.append("stdio")
        yield object()

    @asynccontextmanager
    async def fake_http_session(self):
        used_transports.append("streamable-http")
        yield object()

    async def fake_get(self, url, *args, **kwargs):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(client_module.MCPClient, "_stdio_session", fake_stdio_session)
    monkeypatch.setattr(client_module.MCPClient, "_streamable_http_session", fake_http_session)
    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    async with client.session():
        pass

    assert used_transports == ["stdio"]


@pytest.mark.anyio
async def test_client_does_not_fallback_when_http_transport_is_explicit(monkeypatch):
    client_module = _load_client_module(monkeypatch)
    MCPClient = client_module.MCPClient
    client = MCPClient(transport="streamable-http")
    used_transports: list[str] = []

    @asynccontextmanager
    async def fake_http_session(self):
        used_transports.append("streamable-http")
        yield object()

    @asynccontextmanager
    async def fake_stdio_session(self):
        used_transports.append("stdio")
        yield object()

    monkeypatch.setattr(client_module.MCPClient, "_streamable_http_session", fake_http_session)
    monkeypatch.setattr(client_module.MCPClient, "_stdio_session", fake_stdio_session)

    async with client.session():
        pass

    assert used_transports == ["streamable-http"]
