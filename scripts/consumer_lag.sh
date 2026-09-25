#!/usr/bin/env bash
# Shows lag per partition for one consumer group, or all of them.
#
# Usage:
#   ./scripts/consumer_lag.sh                 # every group, once
#   ./scripts/consumer_lag.sh trending        # one group, once
#   ./scripts/consumer_lag.sh trending 2      # refresh every 2 seconds

set -euo pipefail

GROUP="${1:-}"
INTERVAL="${2:-0}"
BROKER="${KAFKA_INTERNAL_BROKER:-localhost:29092}"
CONTAINER="${KAFKA_CONTAINER:-kafka-1}"

kafka_groups() {
    docker exec "$CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server "$BROKER" "$@"
}

show() {
    if [ -z "$GROUP" ]; then
        local groups
        groups=$(kafka_groups --list | grep -v '^$' | sort)
        if [ -z "$groups" ]; then
            echo "no consumer groups yet"
            return
        fi
        for group in $groups; do
            echo "=== $group ==="
            kafka_groups --describe --group "$group" || true
            echo
        done
    else
        kafka_groups --describe --group "$GROUP"
    fi
}

if [ "$INTERVAL" -gt 0 ] 2>/dev/null; then
    while true; do
        clear
        date '+%H:%M:%S'
        show
        sleep "$INTERVAL"
    done
else
    show
fi
