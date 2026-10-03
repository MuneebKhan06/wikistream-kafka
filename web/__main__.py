"""Start the dashboard: python -m web

WEB_HOST and WEB_PORT choose where it listens (default 127.0.0.1:8050).
"""

import os
import sys
from pathlib import Path

# Also runnable as a file, which is how scripts/pipeline.sh starts every process.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn  # noqa: E402

from common.metrics import setup_logging  # noqa: E402
from web.app import FRONTEND_DIST, create_app  # noqa: E402


def main() -> None:
    log = setup_logging("web")
    if not (FRONTEND_DIST / "index.html").exists():
        log.warning(
            "the dashboard is not built: run `npm install && npm run build` in frontend/. "
            "The API is served either way."
        )
    host = os.getenv("WEB_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_PORT", "8050"))
    uvicorn.run(create_app(), host=host, port=port, log_level="info", access_log=False)


if __name__ == "__main__":
    main()
