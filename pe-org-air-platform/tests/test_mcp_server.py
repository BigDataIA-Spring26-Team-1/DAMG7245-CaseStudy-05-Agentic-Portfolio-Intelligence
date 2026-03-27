from __future__ import annotations

import importlib
import sys
import types


def _install_mcp_test_stubs(monkeypatch) -> None:
    tools_module = types.ModuleType("app.mcp.tools")

    async def _tool(arguments):
        return "{}"

    tools_module.calculate_org_air_score = _tool
    tools_module.get_company_evidence = _tool
    tools_module.generate_justification = _tool
    tools_module.project_ebitda_impact = _tool
    tools_module.run_gap_analysis = _tool
    tools_module.get_portfolio_summary = _tool

    monkeypatch.setitem(sys.modules, "app.mcp.tools", tools_module)
    sys.modules.pop("app.mcp.server", None)
    sys.modules.pop("app.mcp.asgi", None)


def test_server_uses_env_backed_streamable_http_path(monkeypatch):
    _install_mcp_test_stubs(monkeypatch)
    monkeypatch.setenv("MCP_HOST", "0.0.0.0")
    monkeypatch.setenv("MCP_PORT", "8123")
    monkeypatch.setenv("MCP_PATH", "/custom-mcp")

    import app.mcp.server as server_module

    server_module = importlib.reload(server_module)

    assert server_module.MCP_HOST == "0.0.0.0"
    assert server_module.MCP_PORT == 8123
    assert server_module.MCP_PATH == "/custom-mcp"
    assert server_module.mcp.settings.host == "0.0.0.0"
    assert server_module.mcp.settings.port == 8123
    assert server_module.mcp.settings.streamable_http_path == "/custom-mcp"


def test_server_main_runs_streamable_http_without_mount_path(monkeypatch):
    _install_mcp_test_stubs(monkeypatch)
    import app.mcp.server as server_module

    server_module = importlib.reload(server_module)
    monkeypatch.setenv("MCP_TRANSPORT", "streamable-http")

    captured: dict[str, str] = {}

    def fake_run(*, transport):
        captured["transport"] = transport

    monkeypatch.setattr(server_module.mcp, "run", fake_run)

    server_module.main()

    assert captured == {"transport": "streamable-http"}


def test_asgi_app_uses_server_configured_path(monkeypatch):
    _install_mcp_test_stubs(monkeypatch)
    monkeypatch.setenv("MCP_PATH", "/mcp")

    import app.mcp.server as server_module

    server_module = importlib.reload(server_module)

    import app.mcp.asgi as asgi_module

    asgi_module = importlib.reload(asgi_module)

    routes = getattr(asgi_module.app, "routes", [])
    route_paths = {getattr(route, "path", None) for route in routes}

    assert "/mcp" in route_paths
