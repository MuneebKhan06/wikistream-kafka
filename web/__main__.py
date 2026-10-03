"""Start the dashboard: python -m web

WEB_HOST and WEB_PORT choose where it listens (default 127.0.0.1:8000).
"""

import os

import uvicorn

from common.metrics import setup_logging
from web.app import create_app


def main() -> None:
    setup_logging("web")
    host = os.getenv("WEB_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_PORT", "8000"))
    uvicorn.run(create_app(), host=host, port=port, log_level="info", access_log=False)


if __name__ == "__main__":
    main()
