import os
import sys

import structlog

os.environ["MCP_TRANSPORT"] = "stdio"


def configure_stdio_logging() -> None:
    structlog.configure(
        logger_factory=structlog.WriteLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )


configure_stdio_logging()

from app.mcp.server import main


if __name__ == "__main__":
    main()
