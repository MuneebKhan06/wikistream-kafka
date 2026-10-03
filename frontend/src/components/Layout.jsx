import { NavLink, Outlet } from "react-router-dom";
import { usePolling } from "../api.js";
import { Status, healthLabel, healthLevel } from "./Status.jsx";
import { ThemeToggle } from "./ThemeToggle.jsx";

const ICONS = {
  overview: "M2 13V8M6 13V4M10 13V6M14 13V2",
  trending: "M2 12l4-4 3 3 5-6M10 5h4v4",
  edits: "M3 4h10M3 8h10M3 12h6",
  wars: "M3 3l10 10M13 3L3 13",
  pages: "M4 2h6l3 3v9H4zM10 2v3h3",
  pipeline: "M2 8h3M11 8h3M5 5h6v6H5z",
};

export const NAV = [
  { to: "/", label: "Overview", icon: "overview", end: true },
  { to: "/trending", label: "Trending", icon: "trending" },
  { to: "/edits", label: "Live edits", icon: "edits" },
  { to: "/edit-wars", label: "Edit wars", icon: "wars" },
  { to: "/pages", label: "Pages", icon: "pages" },
  { to: "/pipeline", label: "Pipeline", icon: "pipeline" },
];

function Icon({ name }) {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
      <path
        d={ICONS[name]}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function Layout({ routes }) {
  const health = usePolling("/api/health", {}, 15000);
  const available = new Set(routes);

  return (
    <div className="app">
      <aside className="sidebar">
        <NavLink to="/" className="brand" end>
          <img src="/favicon.svg" width="26" height="26" alt="" />
          <span>
            WikiStream
            <small>Live Wikipedia edits</small>
          </span>
        </NavLink>
        <nav aria-label="Sections" style={{ display: "contents" }}>
          {NAV.filter((item) => available.has(item.to)).map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}
            >
              <Icon name={item.icon} />
              {item.label}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer">
          <Status level={health.error && !health.data ? "critical" : healthLevel(health.data)}>
            {healthLabel(health.data, health.error)}
          </Status>
          <ThemeToggle />
        </div>
      </aside>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}
