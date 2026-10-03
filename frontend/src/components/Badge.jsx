const ICONS = {
  bot: "M5 6h6v6H5zM8 3v3M6.5 9h.01M9.5 9h.01",
  revert: "M6 4L3 7l3 3M3 7h7a3 3 0 0 1 0 6H8",
  new: "M8 3v10M3 8h10",
  minor: "M4 8h8",
};

/** A small label with an icon, so the meaning never rests on color. */
export function Badge({ kind, children }) {
  return (
    <span className={`badge badge-${kind}`}>
      <svg width="12" height="12" viewBox="0 0 16 16" aria-hidden="true">
        <path d={ICONS[kind]} fill="none" stroke="currentColor" strokeWidth="1.7"
          strokeLinecap="round" strokeLinejoin="round" />
      </svg>
      {children}
    </span>
  );
}
