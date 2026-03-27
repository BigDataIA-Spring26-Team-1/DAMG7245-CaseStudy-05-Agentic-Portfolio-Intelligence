from __future__ import annotations

from prometheus_client import make_asgi_app
from starlette.applications import Starlette
from starlette.routing import Mount

from app.mcp.server import MCP_PATH, mcp

app = Starlette(
    routes=[
        Mount(MCP_PATH, app=mcp.streamable_http_app()),
        Mount("/metrics", app=make_asgi_app()),
    ]
)
