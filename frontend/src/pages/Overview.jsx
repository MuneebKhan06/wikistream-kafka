import { usePolling } from "../api.js";
import { formatAgo, formatCount, formatDateTime } from "../format.js";

/** What the pipeline has stored, and how recently. */
export function Overview() {
  const overview = usePolling("/api/overview", { minutes: 60 }, 10000);
  const data = overview.data;

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Overview</h1>
          <p>Every edit to every Wikimedia wiki, streamed through Kafka into PostgreSQL.</p>
        </div>
      </header>

      {overview.error && !data && (
        <div className="notice error-text" role="alert">
          Could not load the overview: {overview.error.message}
        </div>
      )}

      {data && (
        <section className={`card${overview.refreshing ? " refreshing" : ""}`}>
          <div className="card-header">
            <div>
              <h2>Stored data</h2>
              <p className="subtitle">What has reached PostgreSQL so far</p>
            </div>
          </div>
          <dl className="facts">
            <dt>Edits stored</dt>
            <dd>{formatCount(data.edits_stored)}</dd>
            <dt>Newest edit</dt>
            <dd>
              {formatDateTime(data.newest_event_time)}{" "}
              <span className="muted">({formatAgo(data.newest_event_time)})</span>
            </dd>
            <dt>Last stored</dt>
            <dd>{formatAgo(data.newest_stored_at)}</dd>
          </dl>
        </section>
      )}
    </>
  );
}
