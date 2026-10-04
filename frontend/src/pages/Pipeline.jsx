import { usePolling } from "../api.js";
import { BarList } from "../charts/BarList.jsx";
import { ChartCard } from "../components/ChartCard.jsx";
import { StatTile } from "../components/StatTile.jsx";
import { Status } from "../components/Status.jsx";
import { formatCount, formatNumber } from "../format.js";

const GROUP_ROLES = {
  cleaner: "Deduplicates and cleans wiki.raw",
  "page-state": "Latest edit per page, compacted",
  "edit-wars": "Revert chains into wiki.alerts",
  trending: "Per minute counts per page. Commits only past closed minutes, so it trails by design",
  storage: "Edits into PostgreSQL",
  "alerts-storage": "Alerts into PostgreSQL",
};

function shortGroup(group) {
  return group.includes(".") ? group.slice(group.indexOf(".") + 1) : group;
}

// Records in flight in a running pipeline: about half a minute of live traffic.
const LIVE_LAG = 1000;

function groupStatus(group) {
  if (group.error) return { level: "critical", label: "Unreadable" };
  const running = group.members > 0;
  // Trending commits only up to the oldest minute it still has open, so it
  // always trails by a couple of minutes of traffic. That is the design, not
  // a backlog.
  if (shortGroup(group.group) === "trending" && running) {
    return { level: "good", label: "Live, holding open minutes" };
  }
  if (group.caught_up || group.lag < LIVE_LAG) {
    return running ? { level: "good", label: "Live" } : { level: "unknown", label: "Caught up, not running" };
  }
  return running
    ? { level: "warning", label: "Catching up" }
    : { level: "serious", label: "Behind, not running" };
}

function FailureTests({ results }) {
  if (!results) return null;
  const rows = Object.entries(results);
  return (
    <section className="card">
      <div className="card-header">
        <div>
          <h2>Failure tests</h2>
          <p className="subtitle">
            Recorded by <code>scripts/failure_tests.py</code>, each in an isolated sandbox and judged
            on the data read back afterwards
          </p>
        </div>
      </div>
      <div className="table-scroll">
        <table className="data">
          <thead>
            <tr>
              <th>Test</th>
              <th>What was done</th>
              <th>Result</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(([name, result]) => (
              <tr key={name}>
                <td className="nowrap">{name.replace(/-/g, " ")}</td>
                <td className="secondary">{result.action}</td>
                <td className="nowrap">
                  <Status level={result.passed ? "good" : "critical"}>
                    {result.passed ? "Passed" : "Failed"}
                  </Status>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Performance({ results }) {
  const throughput = results?.producer_throughput;
  const live = results?.live_stream;
  if (!throughput && !live) return null;
  return (
    <div className="stats">
      {live && (
        <>
          <StatTile label="End to end latency, p50" value={`${(live.latency_ms.p50 / 1000).toFixed(1)} s`}
            detail="edit on the wiki to row in PostgreSQL" />
          <StatTile label="p95" value={`${(live.latency_ms.p95 / 1000).toFixed(1)} s`}
            detail={`p99 ${(live.latency_ms.p99 / 1000).toFixed(1)} s`} />
        </>
      )}
      {throughput && (
        <>
          <StatTile label="Producer, acks=all" value={`${formatCount(throughput.acks_all_idempotent.events_per_sec_median)}/s`}
            detail="idempotent, median of runs" />
          <StatTile label="Producer, acks=1" value={`${formatCount(throughput.acks_1.events_per_sec_median)}/s`}
            detail={`${throughput.acks_1_speedup}x, without the replica wait`} />
        </>
      )}
    </div>
  );
}

/** The cluster and every consumer group, as Kafka reports them right now. */
export function Pipeline() {
  const pipeline = usePolling("/api/pipeline", {}, 10000);
  const benchmarks = usePolling("/api/benchmarks", {}, 0);
  const data = pipeline.data;
  const groups = data?.groups ?? [];
  const brokersUp = data?.brokers.up.length ?? 0;
  const lagRows = groups
    .filter((g) => !g.error)
    .map((g) => ({ key: g.group, label: shortGroup(g.group), value: g.lag, ...g }));

  return (
    <>
      <header className="page-header">
        <div>
          <h1>Pipeline</h1>
          <p>
            Read live from Kafka: which brokers are up, whether every partition has a leader and
            its replicas, and how far each consumer group is behind the topic it reads.
          </p>
        </div>
      </header>

      {pipeline.error && (
        <div className="notice error-text" role="alert" style={{ marginBottom: 16 }}>
          Could not reach Kafka: {pipeline.error.message}
        </div>
      )}

      {data && (
        <div className="stack">
          <div className={`stats${pipeline.refreshing ? " refreshing" : ""}`}>
            <div className="stat">
              <div className="stat-label">Brokers</div>
              <div className="stat-value">
                {brokersUp} of {data.brokers.expected}
              </div>
              <div style={{ marginTop: 6 }}>
                <Status level={brokersUp === data.brokers.expected ? "good" : brokersUp >= 2 ? "serious" : "critical"}>
                  {brokersUp === data.brokers.expected ? "All up" : `Down: ${data.brokers.expected - brokersUp}`}
                </Status>
              </div>
            </div>
            <StatTile label="Controller" value={`Broker ${data.brokers.controller ?? "?"}`} detail="KRaft quorum leader" />
            <div className="stat">
              <div className="stat-label">Partitions</div>
              <div className="stat-value">{data.partitions.total}</div>
              <div style={{ marginTop: 6 }}>
                {data.partitions.leaderless > 0 ? (
                  <Status level="critical">{data.partitions.leaderless} without a leader</Status>
                ) : data.partitions.under_replicated > 0 ? (
                  <Status level="warning">{data.partitions.under_replicated} under-replicated</Status>
                ) : (
                  <Status level="good">All led and in sync</Status>
                )}
              </div>
            </div>
            <StatTile label="Dead letter queue" value={formatCount(data.dlq_records)} detail="records that failed parsing" />
          </div>

          <ChartCard
            title="Consumer lag"
            subtitle="Records each group still has to read. A group that has read everything can show one record per partition: the commit marker that ends a transactional topic."
            refreshing={pipeline.refreshing}
            chart={
              <BarList
                rows={lagRows}
                formatValue={formatCount}
                ariaLabel="Consumer lag by group"
                renderLabel={(row) => {
                  const status = groupStatus(row);
                  return (
                    <span className="group-label">
                      <span>{row.label}</span>
                      <Status level={status.level}>{status.label}</Status>
                    </span>
                  );
                }}
                renderTooltip={(row) => (
                  <>
                    <div className="tooltip-row">
                      <span className="tooltip-key" aria-hidden="true" />
                      <strong>{formatNumber(row.lag)}</strong>
                      <span className="secondary">records behind</span>
                    </div>
                    <div className="muted">
                      {GROUP_ROLES[row.label] ?? row.topic}. Reads {row.topic}, {row.members} member
                      {row.members === 1 ? "" : "s"}, {row.state}.
                    </div>
                  </>
                )}
              />
            }
            table={
              <table className="data">
                <thead>
                  <tr>
                    <th>Group</th>
                    <th>Reads</th>
                    <th>State</th>
                    <th className="right">Members</th>
                    <th className="right">Lag</th>
                    <th>Lag per partition</th>
                  </tr>
                </thead>
                <tbody>
                  {groups.map((g) => (
                    <tr key={g.group}>
                      <td>{shortGroup(g.group)}</td>
                      <td>{g.topic}</td>
                      <td>{g.error ? "unreadable" : g.state}</td>
                      <td className="right">{g.members ?? "n/a"}</td>
                      <td className="right">{g.error ? "n/a" : formatNumber(g.lag)}</td>
                      <td className="secondary num">
                        {(g.partitions ?? []).map((p) => `p${p.partition} ${formatNumber(p.lag)}`).join(", ")}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            }
          />

          <section className="card">
            <div className="card-header">
              <div>
                <h2>Topics</h2>
                <p className="subtitle">Replication factor 3, at least 2 replicas in sync for a write</p>
              </div>
            </div>
            <div className="table-scroll">
              <table className="data">
                <thead>
                  <tr>
                    <th>Topic</th>
                    <th className="right">Partitions</th>
                    <th>Leaders by broker</th>
                    <th>Replicas</th>
                  </tr>
                </thead>
                <tbody>
                  {data.topics.map((t) => (
                    <tr key={t.topic}>
                      <td>{t.topic}</td>
                      <td className="right">{t.partitions}</td>
                      <td className="secondary num">
                        {Object.entries(t.leaders_by_broker)
                          .map(([broker, count]) => (broker === "-1" ? `none: ${count}` : `broker ${broker}: ${count}`))
                          .join(", ")}
                      </td>
                      <td>
                        {t.leaderless > 0 ? (
                          <Status level="critical">{t.leaderless} without a leader</Status>
                        ) : t.under_replicated > 0 ? (
                          <Status level="warning">{t.under_replicated} under-replicated</Status>
                        ) : (
                          <Status level="good">In sync</Status>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          {benchmarks.data && (
            <>
              <Performance results={benchmarks.data.performance} />
              <FailureTests results={benchmarks.data.failure_tests} />
            </>
          )}
        </div>
      )}
    </>
  );
}
