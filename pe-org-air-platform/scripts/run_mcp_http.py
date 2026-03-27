import os

import uvicorn

from app.mcp.asgi import app

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=os.getenv("MCP_HOST", "127.0.0.1"),
        port=int(os.getenv("MCP_PORT", "8000")),
    )