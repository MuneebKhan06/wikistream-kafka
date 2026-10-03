import { useState } from "react";
import { usePolling } from "../api.js";
import { BarList } from "../charts/BarList.jsx";
import { Sparkline } from "../charts/Sparkline.jsx";
import { ChartCard } from "../components/ChartCard.jsx";
import { RangePicker } from "../components/RangePicker.jsx";
import { WikiSelect } from "../components/WikiSelect.jsx";
import { formatAgo, formatCount, formatMinute, formatPercent, wikiUrl } from "../format.js";

function PageLink({ wiki, title }) {
  const url = wikiUrl(wiki, title);
  return (
    <span className="page-label">
      {url ? (
        <a href={url} target="_blank" rel="noreferrer" title={title}>
          <bdi>{title}</bdi>
        </a>
      ) : (
        <bdi title={title}>{title}</bdi>
      )}
      <span className="muted"> {wiki}</span>
    </span>
  );
}

/** Which pages and which wikis are drawing the most edits right now. */
export function Trending() {
  const [minutes, setMinutes] = useState(60);
  const [wiki, setWiki] = useState("");
  const trending = usePolling("/api/trending", { minutes, wiki, limit: 15 }, 15000);
  const wikis = usePolling("/api/wikis", { minutes, limit: 12 }, 15000);

  const pages = trending.data?.pages ?? [];
  const busiest = wikis.data?.wikis ?? [];
  const pageRows = pages.map((p) => ({
    key: `${p.wiki}:${p.title}`,
    label: p.title,
    value: p.edits,
    ...p,
  }));
  const wikiRows = busiest.map((w) => ({ key: w.wiki, label: w.wiki, value: w.edits, ...w }));

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Trending</h1>
          <p>
            Counted per page and per minute by the trending processor. A minute is written once
            its window closes, so this view runs about two minutes behind the live edge.
          </p>
        </div>
        <div className="filters">
          <RangePicker value={minutes} onChange={setMinutes} />
          <WikiSelect value={wiki} onChange={setWiki} minutes={minutes} />
        </div>
      </header>

      {trending.error && (
        <div className="notice error-text" role="alert" style={{ marginBottom: 16 }}>
          Could not refresh trending pages: {trending.error.message}
        </div>
      )}

      <div className="grid grid-wide-narrow">
        <ChartCard
          title="Most edited pages"
          subtitle={
            trending.data?.newest_minute
              ? `Counted up to ${formatMinute(trending.data.newest_minute)} (${formatAgo(
                  trending.data.newest_minute,
                )}). The line shows each page minute by minute.`
              : "Waiting for the first closed minute"
          }
          refreshing={trending.refreshing}
          chart={
            pageRows.length ? (
              <BarList
                rows={pageRows}
                formatValue={formatCount}
                ariaLabel="Most edited pages"
                renderLabel={(row) => <PageLink wiki={row.wiki} title={row.title} />}
                renderExtra={(row) => <Sparkline values={row.per_minute} />}
                renderTooltip={(row) => (
                  <>
                    <div className="tooltip-row">
                      <span className="tooltip-key" aria-hidden="true" />
                      <strong>{formatCount(row.edits)}</strong>
                      <span className="secondary">edits</span>
                    </div>
                    <div>{row.title}</div>
                    <div className="muted">
                      {row.wiki}, busiest minute {formatCount(Math.max(0, ...row.per_minute))}
                    </div>
                  </>
                )}
              />
            ) : (
              <p className="empty">{trending.loading ? "Loading" : "No page edits counted in this window yet."}</p>
            )
          }
          table={
            <table className="data">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Page</th>
                  <th>Wiki</th>
                  <th className="right">Edits</th>
                </tr>
              </thead>
              <tbody>
                {pageRows.map((row, i) => (
                  <tr key={row.key}>
                    <td className="muted">{i + 1}</td>
                    <td>{row.title}</td>
                    <td>{row.wiki}</td>
                    <td className="right">{formatCount(row.edits)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          }
        />

        <ChartCard
          title="Busiest wikis"
          subtitle="All edits stored in the window, from the edits table"
          refreshing={wikis.refreshing}
          chart={
            wikiRows.length ? (
              <BarList
                rows={wikiRows}
                formatValue={formatCount}
                ariaLabel="Busiest wikis"
                renderLabel={(row) => (
                  <button type="button" className="link-button" onClick={() => setWiki(row.wiki)}
                    title={`Show ${row.wiki} pages`}>
                    {row.wiki}
                  </button>
                )}
                renderTooltip={(row) => (
                  <>
                    <div className="tooltip-row">
                      <span className="tooltip-key" aria-hidden="true" />
                      <strong>{formatCount(row.edits)}</strong>
                      <span className="secondary">edits</span>
                    </div>
                    <div className="muted">
                      {row.wiki}, {formatPercent(row.edits ? row.bot_edits / row.edits : 0)} by bots
                    </div>
                  </>
                )}
              />
            ) : (
              <p className="empty">{wikis.loading ? "Loading" : "No edits stored in this window yet."}</p>
            )
          }
          table={
            <table className="data">
              <thead>
                <tr>
                  <th>Wiki</th>
                  <th className="right">Edits</th>
                  <th className="right">By bots</th>
                </tr>
              </thead>
              <tbody>
                {wikiRows.map((row) => (
                  <tr key={row.key}>
                    <td>{row.wiki}</td>
                    <td className="right">{formatCount(row.edits)}</td>
                    <td className="right">
                      {formatPercent(row.edits ? row.bot_edits / row.edits : 0)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          }
        />
      </div>
    </>
  );
}
