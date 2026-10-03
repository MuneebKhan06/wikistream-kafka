/** A single figure with its label: the number is the chart. */
export function StatTile({ label, value, detail, hero = false }) {
  return (
    <div className={`stat${hero ? " stat-hero" : ""}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {detail && <div className="stat-detail">{detail}</div>}
    </div>
  );
}
