#!/usr/bin/env bash
set -euo pipefail

# ================================================================================
# mini-etcd runtime cleanup script (Linux/macOS)
#
# What this script does:
# 1) Stop running node processes found from runtime PID files.
# 2) Kill residual MiniEtcdNodeLauncher processes as fallback when cleaning all profiles.
# 3) Remove runtime artifacts:
#    - data
#    - logs
#    - pid/state files
#
# Usage:
# - clean all profiles: ./clean-runtime.sh
# - clean one profile:  ./clean-runtime.sh --profile=default
# ================================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNTIME_DIR="$SCRIPT_DIR/runtime"
PROFILE=""
NO_PAUSE=false

# Supported formats:
# - --profile=value
# - --profile value
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile=*) PROFILE="${1#*=}"; shift ;;
    --profile) PROFILE="${2:-}"; shift 2 ;;
    --noPause) NO_PAUSE=true; shift ;;
    --noPause=true) NO_PAUSE=true; shift ;;
    --noPause=false) NO_PAUSE=false; shift ;;
    *) echo "[mini-etcd] unknown argument: $1"; exit 1 ;;
  esac
done

echo "[mini-etcd] runtime clean usage:"
echo "[mini-etcd] 1) remove raft data files (runtime/profiles/*/data)"
echo "[mini-etcd] 2) remove runtime logs (runtime/profiles/*/logs)"
echo "[mini-etcd] 3) remove pid/state files (runtime/profiles/*/state)"

if [[ ! -d "$RUNTIME_DIR" ]]; then
  echo "[mini-etcd] runtime directory not found: runtime"
  exit 0
fi

stop_nodes_by_pid_file() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 0
  while IFS='|' read -r _node_id node_pid _rest; do
    if [[ -n "${node_pid:-}" ]]; then
      kill "$node_pid" >/dev/null 2>&1 || true
      sleep 0.2
      kill -9 "$node_pid" >/dev/null 2>&1 || true
      echo "[mini-etcd] stopped node pid=$node_pid"
    fi
  done < "$pid_file"
  rm -f "$pid_file" >/dev/null 2>&1 || true
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

if [[ -z "$PROFILE" ]]; then
  # Clean all profiles under runtime/profiles.
  echo "[mini-etcd] cleaning all runtime artifacts under: runtime"
  if [[ -d "$RUNTIME_DIR/profiles" ]]; then
    for profile_dir in "$RUNTIME_DIR"/profiles/*; do
      [[ -d "$profile_dir" ]] || continue
      stop_nodes_by_pid_file "$profile_dir/state/cluster.pids"
    done
  fi
  stop_residual_launcher_processes
  rm -rf "$RUNTIME_DIR"
  mkdir -p "$RUNTIME_DIR"
  exit 0
fi

PROFILE_RUNTIME_DIR="$RUNTIME_DIR/profiles/$PROFILE"
if [[ -d "$PROFILE_RUNTIME_DIR" ]]; then
  # Clean only one target profile.
  echo "[mini-etcd] cleaning runtime profile: runtime/profiles/$PROFILE"
  stop_nodes_by_pid_file "$PROFILE_RUNTIME_DIR/state/cluster.pids"
  # Profile cleanup must not kill Java processes from other profiles.
  # Global residual cleanup is only safe when cleaning the whole runtime directory.
  rm -rf "$PROFILE_RUNTIME_DIR"
else
  echo "[mini-etcd] profile runtime directory not found: runtime/profiles/$PROFILE"
fi
