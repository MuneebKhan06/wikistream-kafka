/**
 * A status is a colored dot and a label together: the color is never the
 * only carrier of the meaning.
 */
export function Status({ level = "unknown", children }) {
  return (
    <span className={`status status-${level}`}>
      <span className="status-dot" aria-hidden="true" />
      {children}
    </span>
  );
}

export function healthLevel(health) {
  if (!health) return "unknown";
  if (health.database && health.kafka) return "good";
  if (health.database || health.kafka) return "serious";
  return "critical";
}

export function healthLabel(health, error) {
  if (error && !health) return "API unreachable";
  if (!health) return "Checking";
  if (health.database && health.kafka) return "All systems up";
  if (!health.database && !health.kafka) return "Kafka and PostgreSQL down";
  return health.kafka ? "PostgreSQL down" : "Kafka down";
}
