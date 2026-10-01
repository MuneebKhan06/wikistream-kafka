#!/usr/bin/env bash
# Starts, stops and reports on the pipeline's processes.
#
# Usage:
#   ./scripts/pipeline.sh start                  # everything
#   ./scripts/pipeline.sh start cleaner trending # only these
#   ./scripts/pipeline.sh stop                   # graceful: each finishes its batch
#   ./scripts/pipeline.sh restart cleaner
#   ./scripts/pipeline.sh status
#   ./scripts/pipeline.sh logs cleaner           # follow one log
#
# Each process writes to logs/<name>.log and its pid to run/<name>.pid.
# Stopping sends SIGTERM, which every process handles by finishing the
# transaction or batch in hand and committing before it exits.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$ROOT/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON=python3
LOG_DIR="$ROOT/logs"
RUN_DIR="$ROOT/run"
STOP_TIMEOUT_SEC="${STOP_TIMEOUT_SEC:-60}"

cd "$ROOT"

# Start order follows the data: the source first, sinks last.
NAMES=(ingestor cleaner page_state edit_war trending postgres_sink alerts_sink)

script_for() {
    case "$1" in
        ingestor) echo "ingestor/main.py" ;;
        cleaner) echo "processors/cleaner.py" ;;
        page_state) echo "processors/page_state.py" ;;
        edit_war) echo "processors/edit_war.py" ;;
        trending) echo "processors/trending.py" ;;
        postgres_sink) echo "sinks/postgres_sink.py" ;;
        alerts_sink) echo "sinks/alerts_sink.py" ;;
        *)
            echo "unknown process: $1 (known: ${NAMES[*]})" >&2
            return 1
            ;;
    esac
}

pid_of() {
    local file="$RUN_DIR/$1.pid"
    [ -f "$file" ] || return 1
    local pid
    pid=$(cat "$file")
    # A pid file can outlive its process; only a live process counts.
    kill -0 "$pid" 2>/dev/null || return 1
    echo "$pid"
}

start_one() {
    local name="$1" script pid
    script=$(script_for "$name")
    if pid=$(pid_of "$name"); then
        echo "$name already running (pid $pid)"
        return
    fi
    mkdir -p "$LOG_DIR" "$RUN_DIR"
    # Backgrounding the command itself, not a list containing it, so $! is
    # the python process. Backgrounding `cd ... && python ...` would record
    # the wrapping subshell, and stop would kill that and leave python running.
    nohup "$PYTHON" "$ROOT/$script" >>"$LOG_DIR/$name.log" 2>&1 &
    echo $! >"$RUN_DIR/$name.pid"
    echo "$name started (pid $(cat "$RUN_DIR/$name.pid"))"
}

stop_one() {
    local name="$1" pid
    script_for "$name" >/dev/null
    if ! pid=$(pid_of "$name"); then
        echo "$name not running"
        rm -f "$RUN_DIR/$name.pid"
        return
    fi
    kill -TERM "$pid"
    local waited=0
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge "$STOP_TIMEOUT_SEC" ]; then
            echo "$name did not stop within ${STOP_TIMEOUT_SEC}s (pid $pid still running)"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    rm -f "$RUN_DIR/$name.pid"
    echo "$name stopped after ${waited}s"
}

status_one() {
    local name="$1" pid last
    last=""
    [ -f "$LOG_DIR/$name.log" ] && last=$(tail -n 1 "$LOG_DIR/$name.log" | cut -c1-110)
    if pid=$(pid_of "$name"); then
        printf "%-14s running  pid %-8s %s\n" "$name" "$pid" "$last"
    else
        printf "%-14s stopped               %s\n" "$name" "$last"
    fi
}

selected() {
    if [ "$#" -gt 0 ]; then
        echo "$@"
    else
        echo "${NAMES[@]}"
    fi
}

ACTION="${1:-status}"
shift || true

case "$ACTION" in
    start)
        for name in $(selected "$@"); do start_one "$name"; done
        ;;
    stop)
        # Same order as start: the source stops first, then each stage
        # downstream of it, so nothing new arrives behind a stopping process.
        for name in $(selected "$@"); do stop_one "$name"; done
        ;;
    restart)
        for name in $(selected "$@"); do
            stop_one "$name"
            start_one "$name"
        done
        ;;
    status)
        for name in $(selected "$@"); do status_one "$name"; done
        ;;
    logs)
        name="${1:?which process? one of: ${NAMES[*]}}"
        script_for "$name" >/dev/null
        tail -n 50 -f "$LOG_DIR/$name.log"
        ;;
    *)
        echo "usage: $0 {start|stop|restart|status|logs} [process ...]" >&2
        exit 1
        ;;
esac
