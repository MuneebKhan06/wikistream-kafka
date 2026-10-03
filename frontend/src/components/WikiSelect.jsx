import { usePolling } from "../api.js";
import { formatCount } from "../format.js";

/** Pick one wiki, from those active in the window, busiest first. */
export function WikiSelect({ value, onChange, minutes = 60 }) {
  const wikis = usePolling("/api/wikis", { minutes, limit: 40 }, 30000);
  const options = wikis.data?.wikis ?? [];
  const known = options.some((w) => w.wiki === value);

  return (
    <label className="field">
      Wiki
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        <option value="">All wikis</option>
        {value && !known && <option value={value}>{value}</option>}
        {options.map((w) => (
          <option key={w.wiki} value={w.wiki}>
            {w.wiki} ({formatCount(w.edits)})
          </option>
        ))}
      </select>
    </label>
  );
}
