#!/usr/bin/env bash
# Failure test helper: take one broker down and watch the cluster react.
#
# Usage:
#   ./scripts/kill_broker.sh kill 2     # SIGKILL, like a crash or power loss
#   ./scripts/kill_broker.sh stop 2     # graceful shutdown, leadership handed off first
#   ./scripts/kill_broker.sh start 2    # bring it back, wait for replicas to catch up
#   ./scripts/kill_broker.sh status     # leaders per broker and partition health
#   ./scripts/kill_broker.sh rebalance  # hand leadership back to preferred replicas
#
# kill and stop time how long it takes until no pipeline partition is led by
# the stopped broker and none is left without a leader. start times how long
# until no partition is under-replicated.
#
# Each broker here is also a KRaft controller, so one broker down keeps the
# 2 of 3 controller quorum. Two down loses it, and with min.insync.replicas=2
# writes are refused rather than silently accepted by a single copy.

set -euo pipefail

ACTION="${1:-status}"
ID="${2:-}"
TIMEOUT_SEC="${TIMEOUT_SEC:-90}"
TOPIC_PATTERN='Topic: wiki\.'

alive_container() {
    for i in 1 2 3; do
        if [ "$(docker inspect -f '{{.State.Running}}' "kafka-$i" 2>/dev/null)" = "true" ]; then
            echo "kafka-$i"
            return
        fi
    done
    echo "no broker is running" >&2
    exit 1
}

topics() {
    docker exec "$(alive_container)" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:29092 "$@" 2>/dev/null
}

count() {
    # grep -c exits 1 on zero matches, which set -e would treat as failure.
    grep -c "$@" || true
}

leaders_on() {
    topics --describe | grep "$TOPIC_PATTERN" | count -E "Leader: $1([^0-9]|$)"
}

unavailable() {
    topics --describe --unavailable-partitions | count "$TOPIC_PATTERN"
}

under_replicated() {
    topics --describe --under-replicated-partitions | count "$TOPIC_PATTERN"
}

now_ms() {
    date +%s%3N
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$ROOT/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON=python3

watch_cluster() {
    # Timing runs in Python against the admin client: polling with the Kafka
    # command line tools starts a JVM per call and would swamp the result.
    "$PYTHON" "$ROOT/admin/wait_for_leaders.py" "$@"
}

require_id() {
    if [[ ! "$ID" =~ ^[123]$ ]]; then
        echo "broker id must be 1, 2 or 3" >&2
        exit 1
    fi
}

status() {
    local described
    described=$(topics --describe | grep "$TOPIC_PATTERN")
    # Match "Partition: N" only; topic header lines contain "PartitionCount".
    echo "pipeline partitions: $(echo "$described" | count -E "Partition: [0-9]")"
    for i in 1 2 3; do
        local state
        state=$(docker inspect -f '{{.State.Status}}' "kafka-$i" 2>/dev/null || echo missing)
        printf "  broker %s  %-8s leads %s\n" "$i" "$state" \
            "$(echo "$described" | count -E "Leader: $i([^0-9]|$)")"
    done
    echo "without a leader:  $(unavailable)"
    echo "under-replicated:  $(under_replicated)"
    docker exec "$(alive_container)" /opt/kafka/bin/kafka-metadata-quorum.sh \
        --bootstrap-server localhost:29092 describe --status 2>/dev/null \
        | grep -E "LeaderId|CurrentVoters" | sed 's/^/controller /' || true
}

take_down() {
    local mode="$1"
    require_id
    local before
    before=$(leaders_on "$ID")
    echo "broker $ID leads $before pipeline partitions, sending $mode"

    local started elapsed
    started=$(now_ms)
    # In the background: docker stop only returns once the process has fully
    # exited, and leadership moves well before that. Watching in parallel
    # measures the move itself rather than the shutdown.
    docker "$mode" "kafka-$ID" >/dev/null &
    local docker_pid=$!

    if ! elapsed=$(watch_cluster away "$ID" "$started" "$TIMEOUT_SEC"); then
        wait "$docker_pid" || true
        echo "leadership did not move off broker $ID: $elapsed"
        status
        exit 1
    fi
    wait "$docker_pid" || true
    echo "leadership moved off broker $ID in $elapsed ms"
    status
}

bring_up() {
    require_id
    local started elapsed
    started=$(now_ms)
    docker start "kafka-$ID" >/dev/null
    echo "broker $ID started, waiting for its replicas to catch up"

    if ! elapsed=$(watch_cluster insync "$started" "$TIMEOUT_SEC"); then
        echo "replicas did not catch up: $elapsed"
        status
        exit 1
    fi
    echo "all replicas back in sync after $elapsed ms"
    echo "(leadership returns to preferred replicas on the next automatic rebalance)"
    status
}

rebalance() {
    # A restarted broker comes back as a follower everywhere, so leadership
    # stays piled on the brokers that stayed up until Kafka's periodic
    # rebalance runs. This triggers it now.
    docker exec "$(alive_container)" /opt/kafka/bin/kafka-leader-election.sh \
        --bootstrap-server localhost:29092 --election-type PREFERRED \
        --all-topic-partitions >/dev/null 2>&1 || true
    status
}

case "$ACTION" in
    kill) take_down kill ;;
    stop) take_down stop ;;
    start) bring_up ;;
    status) status ;;
    rebalance) rebalance ;;
    *)
        echo "usage: $0 {kill|stop|start|status|rebalance} [broker id]" >&2
        exit 1
        ;;
esac
