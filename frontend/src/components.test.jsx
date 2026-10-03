import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LineChart } from "./charts/LineChart.jsx";
import { ChartCard } from "./components/ChartCard.jsx";
import { Overview } from "./pages/Overview.jsx";

const POINTS = [
  { x: "2026-10-03T11:00:00Z", y: 10 },
  { x: "2026-10-03T11:01:00Z", y: 25 },
  { x: "2026-10-03T11:02:00Z", y: 40 },
];

function mockFetch(routes) {
  return vi.spyOn(global, "fetch").mockImplementation(async (url) => {
    const path = String(url).split("?")[0];
    const body = routes[path];
    return {
      ok: body !== undefined,
      status: body !== undefined ? 200 : 404,
      statusText: "",
      json: async () => body ?? { detail: "missing" },
    };
  });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("LineChart", () => {
  it("labels the last value and moves a crosshair with the arrow keys", () => {
    render(<LineChart points={POINTS} formatY={String} formatX={(x) => x.slice(11, 16)} valueLabel="edits" ariaLabel="test chart" />);
    const chart = screen.getByRole("img", { name: "test chart" });
    expect(chart).toHaveTextContent("40");

    chart.focus();
    fireEvent.keyDown(chart, { key: "ArrowLeft" });
    const tooltip = screen.getByRole("status");
    expect(tooltip).toHaveTextContent("40");
    fireEvent.keyDown(chart, { key: "ArrowLeft" });
    expect(screen.getByRole("status")).toHaveTextContent("25");
    expect(screen.getByRole("status")).toHaveTextContent("11:01");

    fireEvent.keyDown(chart, { key: "Escape" });
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("draws an empty frame without data", () => {
    render(<LineChart points={[]} ariaLabel="empty chart" />);
    expect(screen.getByRole("img", { name: "empty chart" })).toBeInTheDocument();
  });
});

describe("ChartCard", () => {
  it("switches between the chart and its table twin", () => {
    render(<ChartCard title="T" chart={<p>the chart</p>} table={<p>the table</p>} />);
    expect(screen.getByText("the chart")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Show table" }));
    expect(screen.getByText("the table")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Show chart" })).toHaveAttribute("aria-pressed", "true");
  });
});

describe("Overview", () => {
  const base = {
    minutes: 60,
    edits_per_second: 30.2,
    edits: 3979,
    pages: 2927,
    wikis: 89,
    bot_share: 0.39,
    edit_wars: 0,
    edits_stored: 613200,
    newest_stored_at: new Date().toISOString(),
    newest_event_time: new Date().toISOString(),
    per_minute: POINTS.map((p) => ({ minute: p.x, edits: p.y })),
  };

  it("shows the live figures", async () => {
    mockFetch({ "/api/overview": base });
    render(<MemoryRouter><Overview /></MemoryRouter>);
    expect(await screen.findByText("30.2")).toBeInTheDocument();
    expect(screen.getByText("3,979")).toBeInTheDocument();
    expect(screen.getByText("39%")).toBeInTheDocument();
  });

  it("explains an empty window instead of showing zeros alone", async () => {
    mockFetch({ "/api/overview": { ...base, edits: 0, newest_event_time: "2026-09-25T13:27:56Z" } });
    render(<MemoryRouter><Overview /></MemoryRouter>);
    expect(await screen.findByText(/No edits in this window/)).toBeInTheDocument();
    expect(screen.getByText(/pipeline.sh start/)).toBeInTheDocument();
  });

  it("asks again when the range changes", async () => {
    const fetchSpy = mockFetch({ "/api/overview": base });
    render(<MemoryRouter><Overview /></MemoryRouter>);
    await screen.findByText("30.2");
    fireEvent.click(screen.getByRole("radio", { name: "6 hours" }));
    await waitFor(() =>
      expect(fetchSpy.mock.calls.some(([url]) => String(url).includes("minutes=360"))).toBe(true),
    );
  });
});

describe("BarList", () => {
  it("shows a row's tooltip on keyboard focus and names each row for screen readers", async () => {
    const { BarList } = await import("./charts/BarList.jsx");
    render(
      <BarList
        rows={[
          { key: "a", label: "Paris", value: 40 },
          { key: "b", label: "Rome", value: 10 },
        ]}
        ariaLabel="pages"
        renderTooltip={(row) => <span>tip for {row.label}</span>}
      />,
    );
    const rows = screen.getAllByRole("listitem");
    expect(rows[0]).toHaveAccessibleName("Paris: 40");
    fireEvent.focus(rows[1]);
    expect(screen.getByRole("status")).toHaveTextContent("tip for Rome");
    fireEvent.blur(rows[1]);
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });
});

describe("Trending", () => {
  it("lists pages with links to their wiki and filters by wiki", async () => {
    const { Trending } = await import("./pages/Trending.jsx");
    const fetchSpy = mockFetch({
      "/api/trending": {
        minutes: 60,
        newest_minute: new Date().toISOString(),
        pages: [{ wiki: "enwiki", title: "New York", edits: 12, per_minute: [1, 2, 9] }],
      },
      "/api/wikis": { minutes: 60, wikis: [{ wiki: "enwiki", edits: 900, bot_edits: 300 }] },
    });
    render(<MemoryRouter><Trending /></MemoryRouter>);
    const link = await screen.findByRole("link", { name: "New York" });
    expect(link).toHaveAttribute("href", "https://en.wikipedia.org/wiki/New_York");

    fireEvent.click(await screen.findByRole("button", { name: "enwiki" }));
    await waitFor(() =>
      expect(fetchSpy.mock.calls.some(([url]) => String(url).includes("wiki=enwiki"))).toBe(true),
    );
  });
});
