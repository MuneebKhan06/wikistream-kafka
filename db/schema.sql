CREATE TABLE IF NOT EXISTS edits (
    event_id     TEXT PRIMARY KEY,
    wiki         TEXT        NOT NULL,
    title        TEXT        NOT NULL,
    type         TEXT        NOT NULL,
    namespace    INTEGER     NOT NULL,
    username     TEXT        NOT NULL,
    bot          BOOLEAN     NOT NULL,
    minor        BOOLEAN     NOT NULL,
    comment      TEXT        NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL,
    rev_old      BIGINT,
    rev_new      BIGINT,
    length_old   INTEGER,
    length_new   INTEGER,
    server_name  TEXT        NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_edits_page_time ON edits (wiki, title, event_time);
CREATE INDEX IF NOT EXISTS idx_edits_time ON edits (event_time);

CREATE TABLE IF NOT EXISTS trending_minutes (
    wiki        TEXT        NOT NULL,
    title       TEXT        NOT NULL,
    minute      TIMESTAMPTZ NOT NULL,
    edit_count  INTEGER     NOT NULL,
    PRIMARY KEY (wiki, title, minute)
);

CREATE INDEX IF NOT EXISTS idx_trending_minute ON trending_minutes (minute);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id      TEXT PRIMARY KEY,
    wiki          TEXT        NOT NULL,
    title         TEXT        NOT NULL,
    revert_count  INTEGER     NOT NULL,
    users         TEXT[]      NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_page ON alerts (wiki, title);
