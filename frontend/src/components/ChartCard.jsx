import { useState } from "react";

/**
 * A card holding one chart and its table twin. The table is the accessible
 * equivalent of the chart: every value the chart shows can be read there
 * without a pointer.
 */
export function ChartCard({ title, subtitle, refreshing, chart, table, actions }) {
  const [view, setView] = useState("chart");
  return (
    <section className={`card${refreshing ? " refreshing" : ""}`}>
      <div className="card-header">
        <div>
          <h2>{title}</h2>
          {subtitle && <p className="subtitle">{subtitle}</p>}
        </div>
        <div className="card-actions">
          {actions}
          {table && (
            <button
              type="button"
              className="icon-button"
              onClick={() => setView(view === "chart" ? "table" : "chart")}
              aria-pressed={view === "table"}
            >
              {view === "chart" ? "Show table" : "Show chart"}
            </button>
          )}
        </div>
      </div>
      {view === "chart" || !table ? chart : <div className="table-scroll">{table}</div>}
    </section>
  );
}
