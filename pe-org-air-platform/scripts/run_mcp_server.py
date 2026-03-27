import os

os.environ["MCP_TRANSPORT"] = "stdio"

from app.mcp.server import main


if __name__ == "__main__":
    main()
