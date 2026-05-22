#!/usr/bin/env bash
set -euo pipefail

# ================================================================================
# mini-etcd cluster startup script (Linux/macOS)
#
# What this script does:
# 1) Parse startup arguments.
# 2) Prepare runtime directories under scripts/runtime.
# 3) Build etcd-kernel via Maven.
# 4) Start N nodes with MiniEtcdNodeLauncher.
# 5) Keep this terminal alive to manage node lifecycle (unless --noHold=true).
# 6) Stop started nodes automatically on Ctrl+C.
# ================================================================================

# ==================== Paths (relative) ====================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNTIME_DIR="$SCRIPT_DIR/runtime"

# ==================== Defaults ====================
CLUSTER_SIZE=3
HOST="127.0.0.1"
BASE_PORT=2379
PROFILE="default"
DATA_ROOT=""
LOG_DIR=""
ELECTION_TIMEOUT_TICKS=10
HEARTBEAT_TIMEOUT_TICKS=3
SNAPSHOT_TRIGGER_LOG_COUNT=50
NO_HOLD=false

# ==================== Parse Args ====================
# Supported format: --key=value
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clusterSize=*) CLUSTER_SIZE="${1#*=}"; shift ;;
    --host=*) HOST="${1#*=}"; shift ;;
    --basePort=*) BASE_PORT="${1#*=}"; shift ;;
    --profile=*) PROFILE="${1#*=}"; shift ;;
    --dataRoot=*) DATA_ROOT="${1#*=}"; shift ;;
    --logDir=*) LOG_DIR="${1#*=}"; shift ;;
    --electionTimeoutTicks=*) ELECTION_TIMEOUT_TICKS="${1#*=}"; shift ;;
    --heartbeatTimeoutTicks=*) HEARTBEAT_TIMEOUT_TICKS="${1#*=}"; shift ;;
    --snapshotTriggerLogCount=*) SNAPSHOT_TRIGGER_LOG_COUNT="${1#*=}"; shift ;;
    --noHold) NO_HOLD=true; shift ;;
    --noHold=*) NO_HOLD="${1#*=}"; shift ;;
    *) echo "[mini-etcd] unknown argument: $1"; exit 1 ;;
  esac
done

# ==================== Validate Args ====================
if ! [[ "$CLUSTER_SIZE" =~ ^[0-9]+$ ]] || [[ "$CLUSTER_SIZE" -le 0 ]]; then
  echo "[mini-etcd] clusterSize must be a positive integer"
  exit 1
fi
if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]] || [[ "$BASE_PORT" -le 0 ]] || [[ "$BASE_PORT" -gt 65535 ]]; then
  echo "[mini-etcd] basePort must be between 1 and 65535"
  exit 1
fi
if [[ -z "$PROFILE" ]]; then
  echo "[mini-etcd] profile must not be empty"
  exit 1
fi

PROFILE_RUNTIME_DIR="$RUNTIME_DIR/profiles/$PROFILE"
STATE_DIR="$PROFILE_RUNTIME_DIR/state"
PID_FILE="$STATE_DIR/cluster.pids"
[[ -z "$DATA_ROOT" ]] && DATA_ROOT="$PROFILE_RUNTIME_DIR/data"
[[ -z "$LOG_DIR" ]] && LOG_DIR="$PROFILE_RUNTIME_DIR/logs"

echo "[mini-etcd] startup config: profile=$PROFILE, clusterSize=$CLUSTER_SIZE, host=$HOST, basePort=$BASE_PORT"

mkdir -p "$STATE_DIR" "$DATA_ROOT" "$LOG_DIR"

# ==================== Helpers ====================
# Stop nodes recorded in PID file and remove pid artifacts.
stop_nodes_by_pid_file() {
  if [[ -f "$PID_FILE" ]]; then
    while IFS='|' read -r node_id node_pid _rest; do
      if [[ -n "${node_pid:-}" ]]; then
        kill "$node_pid" >/dev/null 2>&1 || true
        sleep 0.2
        kill -9 "$node_pid" >/dev/null 2>&1 || true
      fi
      rm -f "$STATE_DIR/${node_id}.pid" >/dev/null 2>&1 || true
    done < "$PID_FILE"
  fi
  rm -f "$STATE_DIR"/*.pid "$PID_FILE" >/dev/null 2>&1 || true
}

# Fallback kill: remove leaked launcher java processes.
stop_residual_launcher_processes() {
  local launcher_pids
  launcher_pids="$(pgrep -f "MiniEtcdNodeLauncher" || true)"
  if [[ -n "$launcher_pids" ]]; then
    while IFS= read -r launcher_pid; do
      [[ -n "$launcher_pid" ]] || continue
      kill "$launcher_pid" >/dev/null 2>&1 || true
      sleep 0.2
      kill -9 "$launcher_pid" >/dev/null 2>&1 || true
    done <<< "$launcher_pids"
  fi
}

if [[ -f "$PID_FILE" ]]; then
  echo "[mini-etcd] stale pid file found, cleaning previous processes first..."
  stop_nodes_by_pid_file
fi
stop_residual_launcher_processes

cd "$PROJECT_ROOT"
echo "[mini-etcd] building kernel module..."
mvn -q -pl etcd-kernel -am -DskipTests package

# ==================== Build Peer Endpoints ====================
# Build peer endpoints text:
# n1@host:port,n2@host:port,...
PEER_ENDPOINTS=""
for ((i=1; i<=CLUSTER_SIZE; i++)); do
  NODE_PORT=$((BASE_PORT + i - 1))
  ITEM="n${i}@${HOST}:${NODE_PORT}"
  [[ -z "$PEER_ENDPOINTS" ]] && PEER_ENDPOINTS="$ITEM" || PEER_ENDPOINTS="${PEER_ENDPOINTS},${ITEM}"
done

: > "$PID_FILE"

# ==================== Start Nodes ====================
# Start each node in background and wait for node PID file as ready signal.
for ((i=1; i<=CLUSTER_SIZE; i++)); do
  NODE_ID="n${i}"
  NODE_PORT=$((BASE_PORT + i - 1))
  NODE_DATA_DIR="$DATA_ROOT/$NODE_ID"
  NODE_LOG_FILE="$LOG_DIR/$NODE_ID.log"
  NODE_PID_FILE="$STATE_DIR/$NODE_ID.pid"
  mkdir -p "$NODE_DATA_DIR"
  rm -f "$NODE_PID_FILE"

  LAUNCH_ARGS="--nodeId=${NODE_ID} --host=${HOST} --port=${NODE_PORT} --peerEndpoints=${PEER_ENDPOINTS} --dataDir=${NODE_DATA_DIR} --pidFile=${NODE_PID_FILE} --electionTimeoutTicks=${ELECTION_TIMEOUT_TICKS} --heartbeatTimeoutTicks=${HEARTBEAT_TIMEOUT_TICKS} --snapshotTriggerLogCount=${SNAPSHOT_TRIGGER_LOG_COUNT}"
  (
    cd "$PROJECT_ROOT/etcd-kernel"
    mvn -q -l "$NODE_LOG_FILE" exec:java \
      -Dexec.mainClass=com.xhj.etcd.kernel.etcd.bootstrap.MiniEtcdNodeLauncher \
      -Dexec.args="$LAUNCH_ARGS"
  ) &

  wait_seconds=0
  while [[ ! -f "$NODE_PID_FILE" ]]; do
    sleep 1
    wait_seconds=$((wait_seconds + 1))
    if [[ "$wait_seconds" -ge 40 ]]; then
      echo "[mini-etcd] node start timeout: $NODE_ID, pid file not found."
      stop_nodes_by_pid_file
      exit 1
    fi
  done

  NODE_PID="$(tr -d '[:space:]' < "$NODE_PID_FILE")"
  if [[ -z "$NODE_PID" ]]; then
    echo "[mini-etcd] empty pid from launcher: $NODE_ID"
    stop_nodes_by_pid_file
    exit 1
  fi
  echo "${NODE_ID}|${NODE_PID}|${HOST}:${NODE_PORT}|${NODE_LOG_FILE}|${NODE_DATA_DIR}" >> "$PID_FILE"
  echo "[mini-etcd] started $NODE_ID on ${HOST}:${NODE_PORT}, pid=$NODE_PID"
done

echo "[mini-etcd] cluster started successfully."
echo "[mini-etcd] logs dir: $LOG_DIR"
echo "[mini-etcd] state dir: $STATE_DIR"

cleanup_and_exit() {
  echo
  echo "[mini-etcd] shutting down cluster..."
  stop_nodes_by_pid_file
  exit 0
}

trap cleanup_and_exit INT TERM

if [[ "$NO_HOLD" == "true" ]] || [[ "${MINI_ETCD_NO_HOLD:-}" == "1" ]]; then
  exit 0
fi

# Keep this terminal alive to host lifecycle management.
echo "[mini-etcd] this terminal keeps lifecycle. press Ctrl+C to stop all nodes and exit."
while true; do
  sleep 1
done
