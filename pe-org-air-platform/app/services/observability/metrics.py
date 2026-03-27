from __future__ import annotations

import inspect
import time
from functools import wraps

try:
    from prometheus_client import (
        Counter,
        Histogram,
        generate_latest as _generate_latest,
        make_asgi_app as _make_asgi_app,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

    class _NoopMetric:
        def labels(self, **kwargs):
            return self

        def inc(self, amount: float = 1.0) -> None:
            return None

        def observe(self, amount: float) -> None:
            return None

    def Counter(*args, **kwargs):  # type: ignore[misc]
        return _NoopMetric()

    def Histogram(*args, **kwargs):  # type: ignore[misc]
        return _NoopMetric()

    def _generate_latest() -> bytes:
        # Keep /metrics usable even when prometheus_client is unavailable.
        return (
            b"# HELP mcp_tool_calls_total Total MCP tool invocations\n"
            b"# TYPE mcp_tool_calls_total counter\n"
            b"mcp_tool_calls_total{tool_name=\"unavailable\",status=\"unavailable\"} 0\n"
            b"# HELP agent_invocations_total Total agent invocations\n"
            b"# TYPE agent_invocations_total counter\n"
            b"agent_invocations_total{agent_name=\"unavailable\",status=\"unavailable\"} 0\n"
            b"# HELP hitl_approvals_total HITL approval requests\n"
            b"# TYPE hitl_approvals_total counter\n"
            b"hitl_approvals_total{reason=\"unavailable\",decision=\"unavailable\"} 0\n"
            b"# HELP cs_client_calls_total Calls to backend services\n"
            b"# TYPE cs_client_calls_total counter\n"
            b"cs_client_calls_total{service=\"unavailable\",endpoint=\"unavailable\",status=\"unavailable\"} 0\n"
        )

    def _make_asgi_app():
        async def app(scope, receive, send):
            if scope["type"] != "http":
                return
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/plain; charset=utf-8")],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": _generate_latest(),
                }
            )

        return app

MCP_TOOL_CALLS = Counter(
    "mcp_tool_calls_total",
    "Total MCP tool invocations",
    ["tool_name", "status"],
)

MCP_TOOL_DURATION = Histogram(
    "mcp_tool_duration_seconds",
    "MCP tool execution duration",
    ["tool_name"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

AGENT_INVOCATIONS = Counter(
    "agent_invocations_total",
    "Total agent invocations",
    ["agent_name", "status"],
)

AGENT_DURATION = Histogram(
    "agent_duration_seconds",
    "Agent execution duration",
    ["agent_name"],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

HITL_APPROVALS = Counter(
    "hitl_approvals_total",
    "HITL approval requests",
    ["reason", "decision"],
)

CS_CLIENT_CALLS = Counter(
    "cs_client_calls_total",
    "Calls to CS1-CS4 services",
    ["service", "endpoint", "status"],
)


def generate_latest_metrics() -> bytes:
    return _generate_latest()


def make_metrics_asgi_app():
    return _make_asgi_app()


def track_mcp_tool(tool_name: str):
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    MCP_TOOL_CALLS.labels(tool_name=tool_name, status="success").inc()
                    return result
                except Exception:
                    MCP_TOOL_CALLS.labels(tool_name=tool_name, status="error").inc()
                    raise
                finally:
                    MCP_TOOL_DURATION.labels(tool_name=tool_name).observe(
                        time.perf_counter() - start
                    )

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                MCP_TOOL_CALLS.labels(tool_name=tool_name, status="success").inc()
                return result
            except Exception:
                MCP_TOOL_CALLS.labels(tool_name=tool_name, status="error").inc()
                raise
            finally:
                MCP_TOOL_DURATION.labels(tool_name=tool_name).observe(
                    time.perf_counter() - start
                )

        return sync_wrapper

    return decorator


def track_agent(agent_name: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                AGENT_INVOCATIONS.labels(agent_name=agent_name, status="success").inc()
                return result
            except Exception:
                AGENT_INVOCATIONS.labels(agent_name=agent_name, status="error").inc()
                raise
            finally:
                AGENT_DURATION.labels(agent_name=agent_name).observe(
                    time.perf_counter() - start
                )

        return wrapper

    return decorator


def track_cs_client(service: str, endpoint: str):
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    result = await func(*args, **kwargs)
                    CS_CLIENT_CALLS.labels(
                        service=service,
                        endpoint=endpoint,
                        status="success",
                    ).inc()
                    return result
                except Exception:
                    CS_CLIENT_CALLS.labels(
                        service=service,
                        endpoint=endpoint,
                        status="error",
                    ).inc()
                    raise

            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                CS_CLIENT_CALLS.labels(
                    service=service,
                    endpoint=endpoint,
                    status="success",
                ).inc()
                return result
            except Exception:
                CS_CLIENT_CALLS.labels(
                    service=service,
                    endpoint=endpoint,
                    status="error",
                ).inc()
                raise

        return sync_wrapper

    return decorator
