import { useEffect, useState } from "react";
import { usePolling } from "../api.js";
import { Badge } from "../components/Badge.jsx";
import { StatTile } from "../components/StatTile.jsx";
import { Status } from "../components/Status.jsx";
import { formatAgo, formatCount, formatNumber, wikiUrl } from "../format.js";

function useDebounced(value, ms = 300) {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setSettled(value), ms);
    return () => clearTimeout(timer);
  }, [value, ms]);
  return settled;
}

/**
 * The latest state of every page, read from the compacted wiki.page-latest
 * topic. The API keeps that topic in memory, so searches never touch the
 * full edit history.
 */
export function Pages() {
  const [query, setQuery] = useState("");
  const [wiki, setWiki] = useState("");
  const q = useDebounced(query.trim());
  const result = usePolling("/api/pages", { q, wiki: wiki.trim(), limit: 25 }, 10000);
  const snapshot = result.data?.snapshot;
  const pages = result.data?.pages ?? [];

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Pages</h1>
          <p>
            One record per page: its latest edit. Kafka log compaction keeps only the newest record
            for each page, and a deletion leaves a tombstone that removes the page entirely.
          </p>
        </div>
      </header>

      {snapshot && (
        <div className="stats" style={{ marginBottom: 16 }}>
          <StatTile label="Pages in the snapshot" value={formatCount(snapshot.pages)} />
          <StatTile label="Records read" value={formatCount(snapshot.records_read)}
            detail="from the start of the topic" />
          <StatTile label="Records per page" value={snapshot.records_per_page ?? "n/a"}
            detail="1.00 once fully compacted" />
          <StatTile label="Deletions applied" value={formatCount(snapshot.tombstones)} detail="tombstones" />
          <div className="stat">
            <div className="stat-label">Snapshot</div>
            <div style={{ marginTop: 10 }}>
              {snapshot.error ? (
                <Status level="critical">Not following: {snapshot.error}</Status>
              ) : snapshot.caught_up ? (
                <Status level="good">Caught up, following live</Status>
              ) : (
                <Status level="warning">Still reading the topic</Status>
              )}
            </div>
          </div>
        </div>
      )}

      <section className={`card${result.refreshing ? " refreshing" : ""}`}>
        <div className="card-header">
          <div className="filters">
            <label className="field">
              Title contains
              <input
                type="search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="for example, Paris"
                aria-label="Search page titles"
              />
            </label>
            <label className="field">
              Wiki
              <input
                type="text"
                value={wiki}
                onChange={(e) => setWiki(e.target.value)}
                placeholder="any, or enwiki"
                aria-label="Limit to one wiki"
                size={12}
              />
            </label>
          </div>
        </div>

        {result.error && <p className="error-text">Search failed: {result.error.message}</p>}
        {pages.length === 0 ? (
          <p className="empty">
            {result.loading ? "Loading" : q ? `No page titles contain “${q}”.` : "No pages yet."}
          </p>
        ) : (
          <div className="table-scroll">
            <table className="data fixed">
              <colgroup>
                <col className="col-page" />
                <col />
                <col className="col-user" />
                <col className="col-rev" />
                <col className="col-num" />
              </colgroup>
              <thead>
                <tr>
                  <th>Page</th>
                  <th>Last change</th>
                  <th>By</th>
                  <th className="right">Revision</th>
                  <th className="right">Size</th>
                </tr>
              </thead>
              <tbody>
                {pages.map((page) => {
                  const url = wikiUrl(page.wiki, page.title);
                  return (
                    <tr key={`${page.wiki}:${page.title}`}>
                      <td>
                        {url ? (
                          <a href={url} target="_blank" rel="noreferrer"><bdi>{page.title}</bdi></a>
                        ) : (
                          <bdi>{page.title}</bdi>
                        )}
                        <div className="muted feed-meta">
                          {page.wiki}
                          {page.type === "new" && <Badge kind="new">new page</Badge>}
                          {page.is_revert && <Badge kind="revert">revert</Badge>}
                        </div>
                      </td>
                      <td>
                        {formatAgo(page.event_time)}
                        <div className="muted feed-comment" title={page.comment}>
                          <bdi>{page.comment}</bdi>
                        </div>
                      </td>
                      <td>
                        <bdi>{page.user}</bdi>
                        {page.bot && <Badge kind="bot">bot</Badge>}
                      </td>
                      <td className="right nowrap">{page.rev_id ?? "n/a"}</td>
                      <td className="right nowrap">
                        {page.length === null ? "n/a" : `${formatNumber(page.length)} B`}
                      </td>
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
