"""The dashboard's HTTP API and the page that uses it.

Run with `python -m web`. Everything is read only: the API reads what the
pipeline stored in PostgreSQL and Kafka and never writes to either.
"""

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from common.config import PROJECT_ROOT, admin_config
from web import cluster, queries
from web.cache import TTLCache
from web.db import Database
from web.pages import PageStore

BENCHMARKS_DIR = PROJECT_ROOT / "benchmarks"

CACHE_SECONDS = 5.0

STATIC_DIR = Path(__file__).resolve().parent / "static"

log = logging.getLogger("web")


def kafka_reachable(admin: AdminClient) -> bool:
    try:
        admin.list_topics(timeout=3)
        return True
    except Exception as exc:
        log.warning("kafka check failed: %s", exc)
        return False


def page_json(page) -> dict:
    return {
        "wiki": page.wiki,
        "title": page.title,
        "event_id": page.event_id,
        "type": page.type,
        "user": page.user,
        "bot": page.bot,
        "minor": page.minor,
        "is_revert": page.is_revert,
        "comment": page.comment,
        "event_time": page.event_time,
        "rev_id": page.rev_id,
        "length": page.length,
    }


def create_app(
    db: Database = None, admin: AdminClient = None, pages: PageStore = None
) -> FastAPI:
    db = db or Database()
    admin = admin or AdminClient(admin_config())
    pages = pages if pages is not None else PageStore()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        pages.start()
        yield
        pages.stop()
        db.close()

    app = FastAPI(
        title="WikiStream",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    app.state.db = db
    app.state.admin = admin
    cache = TTLCache(CACHE_SECONDS)

    @app.get("/api/health")
    def health():
        database = db.ping()
        kafka = kafka_reachable(admin)
        status = "ok" if database and kafka else "degraded"
        body = {"status": status, "database": database, "kafka": kafka}
        return JSONResponse(body, status_code=200 if status == "ok" else 503)

    @app.get("/api/overview")
    def overview(minutes: int = Query(60, ge=1, le=queries.MAX_MINUTES)):
        minutes = queries.clamp_minutes(minutes)
        return cache.get(("overview", minutes), lambda: queries.overview(db, minutes))

    @app.get("/api/trending")
    def trending(
        minutes: int = Query(60, ge=1, le=queries.MAX_MINUTES),
        wiki: Optional[str] = Query(None, max_length=64),
        limit: int = Query(15, ge=1, le=50),
    ):
        minutes = queries.clamp_minutes(minutes)
        return cache.get(
            ("trending", minutes, wiki, limit),
            lambda: queries.trending(db, minutes, wiki, limit),
        )

    @app.get("/api/wikis")
    def wikis(
        minutes: int = Query(60, ge=1, le=queries.MAX_MINUTES),
        limit: int = Query(20, ge=1, le=100),
    ):
        minutes = queries.clamp_minutes(minutes)
        return cache.get(("wikis", minutes, limit), lambda: queries.wikis(db, minutes, limit))

    @app.get("/api/edits")
    def edits(
        limit: int = Query(50, ge=1, le=200),
        wiki: Optional[str] = Query(None, max_length=64),
        humans_only: bool = False,
        reverts_only: bool = False,
        before: Optional[datetime] = None,
    ):
        # Not cached: the feed is the one view meant to be up to the second.
        return queries.latest_edits(db, limit, wiki, humans_only, reverts_only, before)

    @app.get("/api/pipeline")
    def pipeline():
        return cache.get(("pipeline",), lambda: cluster.pipeline(admin))

    @app.get("/api/pages")
    def search_pages(
        q: str = Query("", max_length=200),
        wiki: Optional[str] = Query(None, max_length=64),
        limit: int = Query(20, ge=1, le=100),
    ):
        return {
            "snapshot": pages.stats(),
            "pages": [page_json(page) for page in pages.search(q, wiki, limit)],
        }

    @app.get("/api/pages/{wiki}/{title:path}")
    def get_page(wiki: str, title: str):
        page = pages.get(wiki, title)
        if page is None:
            raise HTTPException(404, "not in the snapshot: never edited, or deleted")
        return page_json(page)

    @app.get("/api/benchmarks")
    def benchmarks():
        results = {}
        for path in sorted(BENCHMARKS_DIR.glob("*.json")):
            try:
                results[path.stem] = json.loads(path.read_text())
            except (OSError, ValueError) as exc:
                log.warning("could not read %s: %s", path.name, exc)
        return results

    @app.get("/api/alerts")
    def alerts(limit: int = Query(50, ge=1, le=200)):
        return cache.get(("alerts", limit), lambda: queries.alerts(db, limit))

    return app
