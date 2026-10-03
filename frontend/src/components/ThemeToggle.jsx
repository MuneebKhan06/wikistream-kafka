import { useEffect, useState } from "react";

const STORAGE_KEY = "wikistream-theme";
const ORDER = ["system", "light", "dark"];

function readStored() {
  try {
    return localStorage.getItem(STORAGE_KEY) || "system";
  } catch {
    return "system";
  }
}

/** Cycles system, light, dark. "system" leaves the choice to the OS setting. */
export function ThemeToggle() {
  const [theme, setTheme] = useState(readStored);

  useEffect(() => {
    const root = document.documentElement;
    if (theme === "system") root.removeAttribute("data-theme");
    else root.setAttribute("data-theme", theme);
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      // Storage can be unavailable; the theme still applies for this visit.
    }
  }, [theme]);

  const next = ORDER[(ORDER.indexOf(theme) + 1) % ORDER.length];
  const label = { system: "Theme: system", light: "Theme: light", dark: "Theme: dark" }[theme];
  return (
    <button
      type="button"
      className="icon-button"
      onClick={() => setTheme(next)}
      title={`Switch to ${next}`}
      aria-label={`${label}. Switch to ${next}`}
    >
      <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
        <circle cx="8" cy="8" r="6.2" fill="none" stroke="currentColor" strokeWidth="1.5" />
        <path d="M8 1.8a6.2 6.2 0 0 1 0 12.4Z" fill="currentColor" />
      </svg>
      {label}
    </button>
  );
}
