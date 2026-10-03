import { useState } from "react";
import { areaPath, linear, linePath, nearestIndex, niceTicks, tickIndexes } from "./scale.js";
import { useWidth } from "./useWidth.js";

const MARGIN = { top: 14, right: 56, bottom: 28, left: 48 };

/**
 * One series over time: a 2px line over a 10% wash, hairline grid, a value
 * at the line's end, and a crosshair that snaps to the nearest minute. The
 * crosshair also follows the arrow keys once the chart has focus.
 */
export function LineChart({
  points,
  height = 220,
  formatX = (x) => String(x),
  formatY = (y) => String(y),
  valueLabel = "value",
  ariaLabel,
}) {
  const [ref, width] = useWidth();
  const [active, setActive] = useState(null);

  const plotWidth = Math.max(width - MARGIN.left - MARGIN.right, 10);
  const plotHeight = height - MARGIN.top - MARGIN.bottom;
  const max = Math.max(0, ...points.map((p) => p.y));
  const ticks = niceTicks(max);
  const top = ticks[ticks.length - 1];

  const x = linear([0, Math.max(points.length - 1, 1)], [0, plotWidth]);
  const y = linear([0, top], [plotHeight, 0]);
  const xy = points.map((p, i) => [x(i), y(p.y)]);
  const xs = xy.map(([px]) => px);

  const last = points.length ? points.length - 1 : null;
  const shown = active ?? null;

  const onPointer = (event) => {
    const bounds = event.currentTarget.getBoundingClientRect();
    const index = nearestIndex(xs, event.clientX - bounds.left - MARGIN.left);
    setActive(index >= 0 ? index : null);
  };

  const onKey = (event) => {
    if (!points.length) return;
    if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
      event.preventDefault();
      const step = event.key === "ArrowLeft" ? -1 : 1;
      // The first press shows the newest point; later presses step from it.
      setActive((current) =>
        current === null ? last : Math.min(Math.max(current + step, 0), last),
      );
    } else if (event.key === "Escape") {
      setActive(null);
    }
  };

  return (
    <div ref={ref} className="chart" style={{ position: "relative" }}>
      <svg
        width={width}
        height={height}
        role="img"
        aria-label={ariaLabel}
        tabIndex={0}
        onPointerMove={onPointer}
        onPointerLeave={() => setActive(null)}
        onKeyDown={onKey}
        onBlur={() => setActive(null)}
        style={{ display: "block", touchAction: "pan-y" }}
      >
        <g transform={`translate(${MARGIN.left},${MARGIN.top})`}>
          {ticks.map((tick) => (
            <g key={tick} transform={`translate(0,${y(tick)})`}>
              <line x1={0} x2={plotWidth} stroke={tick === 0 ? "var(--axis)" : "var(--grid)"} />
              <text x={-8} dy="0.32em" textAnchor="end" className="axis-label">
                {formatY(tick)}
              </text>
            </g>
          ))}
          {tickIndexes(points.length, Math.max(2, Math.floor(plotWidth / 90))).map((i) => (
            <text
              key={i}
              x={x(i)}
              y={plotHeight + 18}
              textAnchor={i === last ? "end" : i === 0 ? "start" : "middle"}
              className="axis-label"
            >
              {formatX(points[i].x)}
            </text>
          ))}

          {points.length > 0 && (
            <>
              <path d={areaPath(xy, plotHeight)} fill="var(--accent-wash)" />
              <path
                d={linePath(xy)}
                fill="none"
                stroke="var(--accent)"
                strokeWidth={2}
                strokeLinejoin="round"
                strokeLinecap="round"
              />
              {shown === null && (
                <>
                  <circle cx={xy[last][0]} cy={xy[last][1]} r={4} fill="var(--accent)"
                    stroke="var(--surface)" strokeWidth={2} />
                  <text x={xy[last][0] + 8} y={xy[last][1]} dy="0.32em" className="end-label">
                    {formatY(points[last].y)}
                  </text>
                </>
              )}
            </>
          )}

          {shown !== null && points[shown] && (
            <g pointerEvents="none">
              <line x1={xy[shown][0]} x2={xy[shown][0]} y1={0} y2={plotHeight}
                stroke="var(--axis)" />
              <circle cx={xy[shown][0]} cy={xy[shown][1]} r={4} fill="var(--accent)"
                stroke="var(--surface)" strokeWidth={2} />
            </g>
          )}
        </g>
      </svg>

      {shown !== null && points[shown] && (
        <div
          className="tooltip"
          role="status"
          style={{
            left: Math.min(MARGIN.left + xy[shown][0] + 12, width - 150),
            top: Math.max(MARGIN.top + xy[shown][1] - 46, 0),
          }}
        >
          <div className="tooltip-row">
            <span className="tooltip-key" aria-hidden="true" />
            <strong>{formatY(points[shown].y)}</strong>
            <span className="secondary">{valueLabel}</span>
          </div>
          <div className="muted">{formatX(points[shown].x)}</div>
        </div>
      )}
    </div>
  );
}
