import { useState } from "react";
import { usePolling } from "../api.js";
import { LineChart } from "../charts/LineChart.jsx";
import { ChartCard } from "../components/ChartCard.jsx";
import { RangePicker } from "../components/RangePicker.jsx";
import { StatTile } from "../components/StatTile.jsx";
import {
  formatAgo,
  formatCount,
  formatDateTime,
  formatMinute,
  formatPercent,
  formatRate,
} from "../format.js";

/**
 * The live picture: how fast edits are arriving, how much has arrived in the
 * chosen window, and the shape of the last N minutes.
 */
export function Overview() {
  const [minutes, setMinutes] = useState(60);
  const overview = usePolling("/api/overview", { minutes }, 10000);
  const data = overview.data;
  const points = (data?.per_minute ?? []).map((p) => ({ x: p.minute, y: p.edits }));
  const empty = data && data.edits === 0;

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Overview</h1>
          <p>Every edit to every Wikimedia wiki, streamed through Kafka into PostgreSQL.</p>
        </div>
        <div className="filters">
          <RangePicker value={minutes} onChange={setMinutes} />
        </div>
      </header>

      {overview.error && (
        <div className="notice error-text" role="alert" style={{ marginBottom: 16 }}>
          {data ? "Showing the last good update. " : ""}Could not refresh: {overview.error.message}
        </div>
      )}

      {empty && (
        <div className="notice" style={{ marginBottom: 16 }}>
          <span>
            <strong>No edits in this window.</strong> The newest stored edit is from{" "}
            {formatDateTime(data.newest_event_time)} ({formatAgo(data.newest_event_time)}). Start
            the pipeline with <code>./scripts/pipeline.sh start</code> to see live traffic.
          </span>
        </div>
      )}

      {data && (
        <div className="stack">
          <div className={`stats${overview.refreshing ? " refreshing" : ""}`}>
            <StatTile
              hero
              label="Edits per second"
              value={formatRate(data.edits_per_second)}
              detail="over the last full minute"
            />
            <StatTile label="Edits" value={formatCount(data.edits)} detail={`last ${label(minutes)}`} />
            <StatTile label="Pages edited" value={formatCount(data.pages)} detail={`on ${formatCount(data.wikis)} wikis`} />
            <StatTile label="Made by bots" value={formatPercent(data.bot_share)} detail="of those edits" />
            <StatTile label="Edit wars" value={formatCount(data.edit_wars)} detail="flagged in this window" />
          </div>

          <ChartCard
            title="Edits per minute"
            subtitle={`Last ${label(minutes)}, by the time each edit was made. Stored ${formatCount(
              data.edits_stored,
            )} edits in total; the newest ${formatAgo(data.newest_stored_at)}.`}
            refreshing={overview.refreshing}
            chart={
              <LineChart
                points={points}
                formatX={formatMinute}
                formatY={formatCount}
                valueLabel="edits"
                ariaLabel={`Edits per minute over the last ${label(minutes)}`}
              />
            }
            table={
              <table className="data">
                <thead>
                  <tr>
                    <th>Minute</th>
                    <th className="right">Edits</th>
                  </tr>
                </thead>
                <tbody>
                  {[...points].reverse().map((p) => (
                    <tr key={p.x}>
                      <td>{formatMinute(p.x)}</td>
                      <td className="right">{formatCount(p.y)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            }
          />
        </div>
      )}
    </>
  );
}

function label(minutes) {
  return minutes < 60 ? `${minutes} minutes` : minutes === 60 ? "hour" : `${minutes / 60} hours`;
}
