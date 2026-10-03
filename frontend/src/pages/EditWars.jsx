import { usePolling } from "../api.js";
import { formatAgo, formatDateTime, wikiUrl } from "../format.js";

/** Pages where several people kept reverting each other. */
export function EditWars() {
  const alerts = usePolling("/api/alerts", { limit: 100 }, 15000);
  const list = alerts.data?.alerts ?? [];

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Edit wars</h1>
          <p>
            Flagged when a page collects three reverts within 30 minutes from at least two
            people, following Wikipedia's three revert rule. Bot reverts do not count: an
            anti vandalism bot undoing vandalism is enforcement, not a war.
          </p>
        </div>
      </header>

      {alerts.error && (
        <div className="notice error-text" role="alert" style={{ marginBottom: 16 }}>
          Could not load edit wars: {alerts.error.message}
        </div>
      )}

      <section className={`card${alerts.refreshing ? " refreshing" : ""}`}>
        <div className="card-header">
          <div>
            <h2>{list.length ? `${list.length} most recent` : "Detected wars"}</h2>
            <p className="subtitle">Newest first, from the alerts table</p>
          </div>
        </div>
        {list.length === 0 ? (
          <p className="empty">{alerts.loading ? "Loading" : "No edit wars detected yet."}</p>
        ) : (
          <div className="table-scroll">
            <table className="data">
              <thead>
                <tr>
                  <th>Page</th>
                  <th className="right">Reverts</th>
                  <th>Who reverted</th>
                  <th>When</th>
                  <th className="right">Lasted</th>
                </tr>
              </thead>
              <tbody>
                {list.map((alert) => {
                  const url = wikiUrl(alert.wiki, alert.title);
                  return (
                    <tr key={alert.alert_id}>
                      <td>
                        {url ? (
                          <a href={url.replace("/wiki/", "/w/index.php?action=history&title=")}
                            target="_blank" rel="noreferrer" title="Open the page history">
                            <bdi>{alert.title}</bdi>
                          </a>
                        ) : (
                          <bdi>{alert.title}</bdi>
                        )}
                        <div className="muted">{alert.wiki}</div>
                      </td>
                      <td className="right">{alert.revert_count}</td>
                      <td>
                        {alert.users.map((user, i) => (
                          <span key={user}>
                            {i > 0 && ", "}
                            <bdi>{user}</bdi>
                          </span>
                        ))}
                      </td>
                      <td>
                        {formatDateTime(alert.window_start)}
                        <div className="muted">{formatAgo(alert.window_end)}</div>
                      </td>
                      <td className="right">{alert.minutes} min</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </>
  );
}
