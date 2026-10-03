/** The arithmetic behind the charts, kept apart from rendering so it can be tested. */

/** Round tick values from 0 up to at least `max`: 0, 1k, 2k... never 0, 1.3k, 2.6k. */
export function niceTicks(max, count = 4) {
  if (!(max > 0)) return [0, 1];
  const raw = max / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => s >= raw);
  const ticks = [];
  for (let value = 0; value < max + step * 0.5; value += step) {
    ticks.push(Number(value.toPrecision(12)));
    if (value >= max) break;
  }
  if (ticks[ticks.length - 1] < max) ticks.push(Number((ticks.length * step).toPrecision(12)));
  return ticks;
}

/** A function mapping [d0, d1] onto [r0, r1]. */
export function linear(domain, range) {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const span = d1 - d0 || 1;
  return (value) => r0 + ((value - d0) / span) * (r1 - r0);
}

/** Index of the point whose x is closest to the pointer, so the crosshair snaps. */
export function nearestIndex(xs, x) {
  if (!xs.length) return -1;
  let best = 0;
  for (let i = 1; i < xs.length; i += 1) {
    if (Math.abs(xs[i] - x) < Math.abs(xs[best] - x)) best = i;
  }
  return best;
}

/** At most `wanted` evenly spaced indexes, always including the last. */
export function tickIndexes(length, wanted = 6) {
  if (length <= 0) return [];
  if (length <= wanted) return Array.from({ length }, (_, i) => i);
  const step = Math.ceil((length - 1) / (wanted - 1));
  const indexes = [];
  for (let i = length - 1; i >= 0; i -= step) indexes.unshift(i);
  return indexes;
}

/** SVG path through points, and the closed area under it down to `baseline`. */
export function linePath(points) {
  return points.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join("");
}

export function areaPath(points, baseline) {
  if (!points.length) return "";
  const first = points[0];
  const last = points[points.length - 1];
  return `${linePath(points)}L${last[0].toFixed(1)},${baseline}L${first[0].toFixed(1)},${baseline}Z`;
}
