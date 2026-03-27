from __future__ import annotations

from starlette.applications import Starlette
from starlette.routing import Mount

from app.mcp.server import MCP_PATH, mcp
from app.services.observability.metrics import make_metrics_asgi_app

app = Starlette(
    routes=[
        Mount(MCP_PATH, app=mcp.streamable_http_app()),
        Mount("/metrics", app=make_metrics_asgi_app()),
    ]
)
