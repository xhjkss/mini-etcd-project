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
NO_PAUSE=false

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
    --noPause) NO_PAUSE=true; shift ;;
    --noPause=true) NO_PAUSE=true; shift ;;
    --noPause=false) NO_PAUSE=false; shift ;;
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
LAST_PORT=$((BASE_PORT + CLUSTER_SIZE - 1))
if [[ "$LAST_PORT" -gt 65535 ]]; then
  echo "[mini-etcd] port range is invalid: basePort=$BASE_PORT, clusterSize=$CLUSTER_SIZE, lastPort=$LAST_PORT."
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
check_command_available() {
  local command_name="$1"
  local command_label="$2"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "[mini-etcd] $command_label command not found in PATH: $command_name"
    exit 1
  fi
}

print_process_command() {
  local process_pid="$1"
  if command -v ps >/dev/null 2>&1; then
    ps -p "$process_pid" -o command= 2>/dev/null | sed "s/^/[mini-etcd] pid $process_pid command: /" || true
  fi
}

ensure_port_free() {
  local check_port="$1"
  local port_pid=""
  if command -v lsof >/dev/null 2>&1; then
    port_pid="$(lsof -tiTCP:"$check_port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true)"
  elif command -v ss >/dev/null 2>&1; then
    port_pid="$(ss -ltnp 2>/dev/null | awk -v port=":$check_port" '$4 ~ port"$" {print $NF}' | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | head -n 1 || true)"
  elif command -v netstat >/dev/null 2>&1; then
    if netstat -an 2>/dev/null | awk -v port=":$check_port" '$0 ~ port && $0 ~ /LISTEN/ {found=1} END {exit found ? 0 : 1}'; then
      echo "[mini-etcd] port $check_port is already in use."
      return 1
    fi
  fi
  if [[ -n "$port_pid" ]]; then
    echo "[mini-etcd] port $check_port is already in use, pid=$port_pid."
    print_process_command "$port_pid"
    return 1
  fi
  return 0
}

check_cluster_ports_free() {
  local port_conflict_found=false
  local check_port
  for ((i=1; i<=CLUSTER_SIZE; i++)); do
    check_port=$((BASE_PORT + i - 1))
    if ! ensure_port_free "$check_port"; then
      port_conflict_found=true
    fi
  done
  if [[ "$port_conflict_found" == "true" ]]; then
    echo "[mini-etcd] requested cluster ports: ${HOST}:${BASE_PORT}-${LAST_PORT}"
    echo "[mini-etcd] how to fix:"
    echo "[mini-etcd] 1. Use another port range, for example: ./start-cluster.sh --basePort=2500"
    echo "[mini-etcd] 2. Or stop the process shown above if it is safe to stop."
    echo "[mini-etcd] 3. If the process is a virtualization/NAT service, prefer changing --basePort."
    return 1
  fi
  return 0
}

print_log_tail() {
  local log_file="$1"
  if [[ -f "$log_file" ]]; then
    echo "[mini-etcd] last 80 lines of log: $log_file"
    tail -n 80 "$log_file" || true
  else
    echo "[mini-etcd] log file not found: $log_file"
  fi
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local wait_seconds="$3"
  local elapsed=0
  while [[ "$elapsed" -lt "$wait_seconds" ]]; do
    if command -v nc >/dev/null 2>&1; then
      nc -z "$host" "$port" >/dev/null 2>&1 && return 0
    else
      (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1 && return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  return 1
}

# Stop nodes recorded in PID file and remove pid artifacts.
stop_nodes_by_pid_file() {
  if [[ -f "$PID_FILE" ]]; then
    while IFS='|' read -r node_id node_pid _rest; do
      if [[ -n "${node_pid:-}" ]]; then
        kill "$node_pid" >/dev/null 2>&1 || true
        sleep 0.2
        kill -9 "$node_pid" >/dev/null 2>&1 || true
        echo "[mini-etcd] stopped $node_id, pid=$node_pid"
      fi
      rm -f "$STATE_DIR/${node_id}.pid" >/dev/null 2>&1 || true
    done < "$PID_FILE"
  fi
  rm -f "$STATE_DIR"/*.pid "$PID_FILE" >/dev/null 2>&1 || true
}

check_command_available "mvn" "Maven"
check_command_available "java" "Java"
check_cluster_ports_free || exit 1

if [[ -f "$PID_FILE" ]]; then
  echo "[mini-etcd] stale pid file found, cleaning previous processes first..."
  stop_nodes_by_pid_file
fi
# Do not run global residual cleanup here: starting one profile must not kill other profiles.
# Use clean-runtime.sh without --profile when a full runtime cleanup is required.

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
start_node() {
  local node_index="$1"
  local node_id="n${node_index}"
  local node_port=$((BASE_PORT + node_index - 1))
  local node_data_dir="$DATA_ROOT/$node_id"
  local node_log_file="$LOG_DIR/$node_id.log"
  local node_pid_file="$STATE_DIR/$node_id.pid"
  local launch_args
  local wait_seconds=0
  local node_pid

  mkdir -p "$node_data_dir"
  rm -f "$node_pid_file"

  launch_args="--nodeId=${node_id} --host=${HOST} --port=${node_port} --peerEndpoints=${PEER_ENDPOINTS} --dataDir=${node_data_dir} --pidFile=${node_pid_file} --electionTimeoutTicks=${ELECTION_TIMEOUT_TICKS} --heartbeatTimeoutTicks=${HEARTBEAT_TIMEOUT_TICKS} --snapshotTriggerLogCount=${SNAPSHOT_TRIGGER_LOG_COUNT}"
  (
    cd "$PROJECT_ROOT/etcd-kernel"
    mvn -q -l "$node_log_file" exec:java \
      -Dexec.mainClass=com.xhj.etcd.kernel.etcd.bootstrap.MiniEtcdNodeLauncher \
      -Dexec.args="$launch_args"
  ) &

  while [[ ! -f "$node_pid_file" ]]; do
    sleep 1
    wait_seconds=$((wait_seconds + 1))
    if [[ "$wait_seconds" -ge 40 ]]; then
      echo "[mini-etcd] node start timeout: $node_id, pid file not found."
      print_log_tail "$node_log_file"
      return 1
    fi
  done

  node_pid="$(tr -d '[:space:]' < "$node_pid_file")"
  if [[ -z "$node_pid" ]]; then
    echo "[mini-etcd] empty pid from launcher: $node_id"
    print_log_tail "$node_log_file"
    return 1
  fi
  if ! kill -0 "$node_pid" >/dev/null 2>&1; then
    echo "[mini-etcd] node process is not running: $node_id, pid=$node_pid"
    print_log_tail "$node_log_file"
    return 1
  fi
  if ! wait_for_port "$HOST" "$node_port" 20; then
    echo "[mini-etcd] node port did not become ready: $node_id, port=$node_port"
    print_log_tail "$node_log_file"
    return 1
  fi

  echo "${node_id}|${node_pid}|${HOST}:${node_port}|${node_log_file}|${node_data_dir}" >> "$PID_FILE"
  echo "[mini-etcd] started $node_id on ${HOST}:${node_port}, pid=$node_pid"
}

for ((i=1; i<=CLUSTER_SIZE; i++)); do
  if ! start_node "$i"; then
    stop_nodes_by_pid_file
    exit 1
  fi
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
