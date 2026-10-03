import { linePath, linear } from "./scale.js";

/**
 * A small trend beside a number: the history in the de-emphasis gray, the
 * latest point in the accent. Decorative next to its value, so hidden from
 * assistive technology; the table view carries the numbers.
 */
export function Sparkline({ values, width = 110, height = 26 }) {
  if (!values || values.length < 2) {
    return <svg width={width} height={height} aria-hidden="true" />;
  }
  const max = Math.max(1, ...values);
  const x = linear([0, values.length - 1], [3, width - 4]);
  const y = linear([0, max], [height - 3, 3]);
  const points = values.map((v, i) => [x(i), y(v)]);
  const last = points[points.length - 1];
  return (
    <svg width={width} height={height} aria-hidden="true" className="sparkline">
      <path d={linePath(points)} fill="none" stroke="var(--deemphasis)" strokeWidth={2}
        strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={last[0]} cy={last[1]} r={3} fill="var(--accent)" stroke="var(--surface)"
        strokeWidth={1.5} />
    </svg>
  );
}
