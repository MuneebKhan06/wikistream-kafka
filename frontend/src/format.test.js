import { describe, expect, it } from "vitest";
import {
  formatAgo,
  formatCount,
  formatDuration,
  formatPercent,
  formatSizeChange,
  wikiUrl,
} from "./format.js";

describe("formatCount", () => {
  it("keeps small numbers whole and compacts large ones", () => {
    expect(formatCount(1284)).toBe("1,284");
    expect(formatCount(12900)).toBe("12.9K");
    expect(formatCount(4200000)).toBe("4.2M");
    expect(formatCount(null)).toBe("–");
  });
});

describe("formatSizeChange", () => {
  it("signs changes the way wiki histories do", () => {
    expect(formatSizeChange(120)).toBe("+120");
    expect(formatSizeChange(-45)).toBe("−45");
    expect(formatSizeChange(0)).toBe("0");
    expect(formatSizeChange(null)).toBe("");
  });
});

describe("durations", () => {
  it("picks a readable unit", () => {
    expect(formatDuration(340)).toBe("340 ms");
    expect(formatDuration(2500)).toBe("2.5 s");
    expect(formatDuration(5 * 60000)).toBe("5 min");
    expect(formatDuration(3 * 86400000)).toBe("3.0 days");
  });

  it("says how long ago something was", () => {
    const now = Date.parse("2026-10-03T12:00:00Z");
    expect(formatAgo("2026-10-03T11:59:59.5Z", now)).toBe("just now");
    expect(formatAgo("2026-10-03T11:58:00Z", now)).toBe("2 min ago");
    expect(formatAgo(null, now)).toBe("never");
  });
});

describe("formatPercent", () => {
  it("formats a fraction", () => {
    expect(formatPercent(0.5478)).toBe("55%");
    expect(formatPercent(0.5478, 1)).toBe("54.8%");
  });
});

describe("wikiUrl", () => {
  it("maps wiki ids to their hosts", () => {
    expect(wikiUrl("enwiki", "New York")).toBe("https://en.wikipedia.org/wiki/New_York");
    expect(wikiUrl("dewiktionary", "Haus")).toBe("https://de.wiktionary.org/wiki/Haus");
    expect(wikiUrl("commonswiki", "Category:Paris")).toBe(
      "https://commons.wikimedia.org/wiki/Category:Paris",
    );
    expect(wikiUrl("zh_yuewiki", "x")).toBe("https://zh-yue.wikipedia.org/wiki/x");
  });

  it("encodes characters that would break a link", () => {
    expect(wikiUrl("enwiki", "C++ (language)?")).toBe(
      "https://en.wikipedia.org/wiki/C%2B%2B_(language)%3F",
    );
  });
});
