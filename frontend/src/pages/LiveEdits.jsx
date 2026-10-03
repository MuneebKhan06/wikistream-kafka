import { useEffect, useRef, useState } from "react";
import { usePolling } from "../api.js";
import { Badge } from "../components/Badge.jsx";
import { WikiSelect } from "../components/WikiSelect.jsx";
import { formatClock, formatDuration, formatSizeChange } from "../format.js";

const REFRESH_MS = 3000;

/** The newest edits as they land in PostgreSQL. */
export function LiveEdits() {
  const [wiki, setWiki] = useState("");
  const [humansOnly, setHumansOnly] = useState(false);
  const [revertsOnly, setRevertsOnly] = useState(false);
  const [paused, setPaused] = useState(false);
  const feed = usePolling(
    "/api/edits",
    { limit: 50, wiki, humans_only: humansOnly, reverts_only: revertsOnly },
    paused ? 0 : REFRESH_MS,
  );

  // Rows that were not in the previous refresh get a brief highlight.
  const seen = useRef(new Set());
  const [fresh, setFresh] = useState(new Set());
  const edits = feed.data?.edits ?? [];
  useEffect(() => {
    if (!feed.data) return;
    const ids = feed.data.edits.map((e) => e.event_id);
    const arrived = seen.current.size ? ids.filter((id) => !seen.current.has(id)) : [];
    seen.current = new Set(ids);
    setFresh(new Set(arrived));
  }, [feed.data]);
  useEffect(() => {
    seen.current = new Set();
  }, [wiki, humansOnly, revertsOnly]);

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Live edits</h1>
          <p>
            The newest edits stored by the PostgreSQL sink, refreshed every few seconds. Each row
            links to the page on its wiki.
          </p>
        </div>
        <div className="filters">
          <WikiSelect value={wiki} onChange={setWiki} />
          <label className="field">
            <input type="checkbox" checked={humansOnly} onChange={(e) => setHumansOnly(e.target.checked)} />
            Humans only
          </label>
          <label className="field">
            <input type="checkbox" checked={revertsOnly} onChange={(e) => setRevertsOnly(e.target.checked)} />
            Reverts only
          </label>
          <button type="button" className="icon-button" onClick={() => setPaused(!paused)} aria-pressed={paused}>
            {paused ? "Resume" : "Pause"}
          </button>
        </div>
      </header>

      {feed.error && (
        <div className="notice error-text" role="alert" style={{ marginBottom: 16 }}>
          Could not refresh the feed: {feed.error.message}
        </div>
      )}

      <section className={`card${feed.refreshing && !paused ? "" : ""}`}>
        <div className="card-header">
          <div>
            <h2>Newest first</h2>
            <p className="subtitle">
              {paused ? "Paused. " : `Updating every ${REFRESH_MS / 1000} seconds. `}
              Hover a time to see how long the edit took to reach PostgreSQL.
            </p>
          </div>
        </div>
        {edits.length === 0 ? (
          <p className="empty">{feed.loading ? "Loading" : "No edits match these filters yet."}</p>
        ) : (
          <div className="table-scroll">
            <table className="data feed fixed">
              <colgroup>
                <col className="col-time" />
                <col className="col-page" />
                <col className="col-user" />
                <col className="col-num" />
                <col />
              </colgroup>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Page</th>
                  <th>User</th>
                  <th className="right">Change</th>
                  <th>Summary</th>
                </tr>
              </thead>
              <tbody aria-live="off">
                {edits.map((edit) => (
                  <tr key={edit.event_id} className={fresh.has(edit.event_id) ? "fresh" : ""}>
                    <td className="num nowrap" title={`Stored ${formatDuration(edit.stored_after_ms)} after the edit`}>
                      {formatClock(edit.event_time)}
                    </td>
                    <td className="feed-page">
                      {edit.url ? (
                        <a href={edit.url} target="_blank" rel="noreferrer">
                          <bdi>{edit.title}</bdi>
                        </a>
                      ) : (
                        <bdi>{edit.title}</bdi>
                      )}
                      <div className="muted feed-meta">
                        {edit.wiki}
                        {edit.type === "new" && <Badge kind="new">new page</Badge>}
                        {edit.is_revert && <Badge kind="revert">revert</Badge>}
                        {edit.type === "log" && <span className="type">· log entry</span>}
                        {edit.type === "categorize" && <span className="type">· category change</span>}
                      </div>
                    </td>
                    <td className="feed-user">
                      <bdi>{edit.user}</bdi>
                      {edit.bot && <Badge kind="bot">bot</Badge>}
                    </td>
                    <td className="right secondary nowrap">{formatSizeChange(edit.size_change)}</td>
                    <td className="feed-comment muted" title={edit.comment}>
                      <bdi>{edit.comment}</bdi>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </>
  );
}
