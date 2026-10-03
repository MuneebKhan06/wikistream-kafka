export const RANGES = [
  { minutes: 15, label: "15 min" },
  { minutes: 60, label: "1 hour" },
  { minutes: 180, label: "3 hours" },
  { minutes: 360, label: "6 hours" },
];

/** The time range for everything below it on the page. */
export function RangePicker({ value, onChange }) {
  return (
    <div className="segmented" role="radiogroup" aria-label="Time range">
      {RANGES.map((range) => (
        <button
          key={range.minutes}
          type="button"
          role="radio"
          aria-checked={value === range.minutes}
          className={value === range.minutes ? "selected" : ""}
          onClick={() => onChange(range.minutes)}
        >
          {range.label}
        </button>
      ))}
    </div>
  );
}
