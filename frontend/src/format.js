/** Number and time formatting shared by every view. */

const compact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const whole = new Intl.NumberFormat("en");

/** 1,284 below ten thousand; 12.9K and 4.2M above it. */
export function formatCount(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "–";
  return Math.abs(value) >= 10000 ? compact.format(value) : whole.format(Math.round(value));
}

export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "–";
  return whole.format(value);
}

export function formatPercent(fraction, digits = 0) {
  if (fraction === null || fraction === undefined || Number.isNaN(fraction)) return "–";
  return `${(fraction * 100).toFixed(digits)}%`;
}

export function formatRate(perSecond) {
  if (perSecond === null || perSecond === undefined) return "–";
  return perSecond >= 100 ? whole.format(Math.round(perSecond)) : perSecond.toFixed(1);
}

/** A signed byte change, as on a wiki's history page: +120, −45. */
export function formatSizeChange(bytes) {
  if (bytes === null || bytes === undefined) return "";
  if (bytes === 0) return "0";
  const sign = bytes > 0 ? "+" : "−";
  return `${sign}${whole.format(Math.abs(bytes))}`;
}

export function formatDuration(ms) {
  if (ms === null || ms === undefined) return "–";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms < 3600000) return `${Math.round(ms / 60000)} min`;
  if (ms < 86400000) return `${(ms / 3600000).toFixed(1)} h`;
  return `${(ms / 86400000).toFixed(1)} days`;
}

/** "12 s ago", "3 min ago", "2.1 h ago". */
export function formatAgo(iso, now = Date.now()) {
  if (!iso) return "never";
  const ms = Math.max(0, now - new Date(iso).getTime());
  return ms < 1500 ? "just now" : `${formatDuration(ms)} ago`;
}

export function formatClock(iso) {
  if (!iso) return "";
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function formatMinute(iso) {
  if (!iso) return "";
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export function formatDateTime(iso) {
  if (!iso) return "";
  return new Date(iso).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

/** Link to a page on its wiki, from the wiki id when the server name is unknown. */
export function wikiUrl(wiki, title) {
  const hosts = {
    commonswiki: "commons.wikimedia.org",
    wikidatawiki: "www.wikidata.org",
    metawiki: "meta.wikimedia.org",
    specieswiki: "species.wikimedia.org",
    mediawikiwiki: "www.mediawiki.org",
  };
  let host = hosts[wiki];
  if (!host) {
    const match = /^([a-z_-]+?)(wiki|wiktionary|wikisource|wikibooks|wikinews|wikiquote|wikivoyage|wikiversity)$/.exec(wiki);
    if (!match) return null;
    const project = match[2] === "wiki" ? "wikipedia" : match[2];
    host = `${match[1].replace(/_/g, "-")}.${project}.org`;
  }
  const path = encodeURIComponent(title.replace(/ /g, "_")).replace(/%3A/g, ":").replace(/%2F/g, "/");
  return `https://${host}/wiki/${path}`;
}
