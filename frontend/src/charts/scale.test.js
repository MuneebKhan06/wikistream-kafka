import { describe, expect, it } from "vitest";
import { areaPath, linear, linePath, nearestIndex, niceTicks, tickIndexes } from "./scale.js";

describe("niceTicks", () => {
  it("steps on round numbers and covers the maximum", () => {
    expect(niceTicks(2423)).toEqual([0, 1000, 2000, 3000]);
    expect(niceTicks(87)).toEqual([0, 25, 50, 75, 100]);
    expect(niceTicks(4)).toEqual([0, 1, 2, 3, 4]);
  });

  it("handles an empty or zero series", () => {
    expect(niceTicks(0)).toEqual([0, 1]);
    expect(niceTicks(undefined)).toEqual([0, 1]);
  });

  it("never leaves the top value above the last tick", () => {
    for (const max of [1, 7, 99, 101, 999, 1001, 12345, 0.3]) {
      const ticks = niceTicks(max);
      expect(ticks[ticks.length - 1]).toBeGreaterThanOrEqual(max);
    }
  });
});

describe("linear", () => {
  it("maps a domain onto a range, including inverted ranges", () => {
    const y = linear([0, 100], [200, 0]);
    expect(y(0)).toBe(200);
    expect(y(50)).toBe(100);
    expect(y(100)).toBe(0);
  });
});

describe("nearestIndex", () => {
  it("snaps to the closest point", () => {
    expect(nearestIndex([0, 10, 20, 30], 14)).toBe(1);
    expect(nearestIndex([0, 10, 20, 30], 16)).toBe(2);
    expect(nearestIndex([], 5)).toBe(-1);
  });
});

describe("tickIndexes", () => {
  it("spreads ticks and keeps the last one", () => {
    expect(tickIndexes(3)).toEqual([0, 1, 2]);
    const ticks = tickIndexes(60, 6);
    expect(ticks.length).toBeLessThanOrEqual(6);
    expect(ticks[ticks.length - 1]).toBe(59);
  });
});

describe("paths", () => {
  it("draws a line and closes the area to the baseline", () => {
    const points = [
      [0, 10],
      [5, 4],
    ];
    expect(linePath(points)).toBe("M0.0,10.0L5.0,4.0");
    expect(areaPath(points, 20)).toBe("M0.0,10.0L5.0,4.0L5.0,20L0.0,20Z");
    expect(areaPath([], 20)).toBe("");
  });
});
