#!/usr/bin/env bash
# Run the CS5 Streamlit Portfolio Intelligence Dashboard
# Usage: ./scripts/run_dashboard.sh [--port PORT]
#
# Prerequisites:
#   1. CS1-CS4 backend services running (FastAPI)
#   2. MCP server running (scripts/run_mcp_http.py or stdio fallback)
#   3. Python environment with all dependencies installed
#
# Environment variables (optional):
#   MCP_CLIENT_TRANSPORT  - "streamable-http" (default) or "stdio"
#   MCP_SERVER_URL        - MCP server URL (default: http://127.0.0.1:8000/mcp)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PORT="${1:-8501}"
if [[ "$1" == "--port" ]] && [[ -n "${2:-}" ]]; then
    PORT="$2"
fi

echo "============================================"
echo "  PE Org-AI-R Portfolio Intelligence"
echo "  CS5 Streamlit Dashboard"
echo "============================================"
echo ""
echo "Project root : $PROJECT_ROOT"
echo "Dashboard    : $PROJECT_ROOT/streamlit/app.py"
echo "Port         : $PORT"
echo ""

cd "$PROJECT_ROOT"

exec streamlit run streamlit/app.py \
    --server.port "$PORT" \
    --server.address "0.0.0.0" \
    --browser.gatherUsageStats false
