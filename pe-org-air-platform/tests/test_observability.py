from __future__ import annotations


def test_metrics_endpoint_exposed(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "mcp_tool_calls_total" in response.text
    assert "agent_invocations_total" in response.text
