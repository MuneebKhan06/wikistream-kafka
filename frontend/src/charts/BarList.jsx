import { useState } from "react";

/**
 * Ranked horizontal bars: label, bar, value at the bar's tip, and an optional
 * extra column (a sparkline). One series, so one color. Each row is its own
 * hit target and shows a tooltip on hover or keyboard focus.
 *
 * Bars are HTML rather than SVG so long labels in any script wrap and
 * truncate the way text does everywhere else.
 */
export function BarList({ rows, formatValue = String, renderLabel, renderExtra, renderTooltip, ariaLabel }) {
  const [active, setActive] = useState(null);
  const max = Math.max(1, ...rows.map((r) => r.value));

  return (
    <ol className="barlist" aria-label={ariaLabel}>
      {rows.map((row, index) => {
        const width = `${Math.max((row.value / max) * 100, 0.5)}%`;
        const hovered = active === index;
        return (
          <li
            key={row.key}
            className={`barlist-row${hovered ? " hovered" : ""}`}
            tabIndex={0}
            onPointerEnter={() => setActive(index)}
            onPointerLeave={() => setActive(null)}
            onFocus={() => setActive(index)}
            onBlur={() => setActive(null)}
            aria-label={`${row.label}: ${formatValue(row.value)}`}
          >
            <div className="barlist-label">{renderLabel ? renderLabel(row) : row.label}</div>
            <div className="barlist-track">
              <div className={`barlist-bar mark${hovered ? " mark-hover" : ""}`} style={{ width }} />
              <span className="barlist-value">{formatValue(row.value)}</span>
            </div>
            {renderExtra && <div className="barlist-extra">{renderExtra(row)}</div>}
            {hovered && renderTooltip && (
              <div className="tooltip barlist-tooltip" role="status">
                {renderTooltip(row)}
              </div>
            )}
          </li>
        );
      })}
    </ol>
  );
}
