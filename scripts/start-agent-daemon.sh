#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source "${PROJECT_ROOT}/scripts/lib-env.sh"
codex_load_env "${PROJECT_ROOT}"

export PYTHONPATH="${PROJECT_ROOT}/.deps${PYTHONPATH:+:${PYTHONPATH}}"
export CODEX_VIEWER_SYNC_MODE="remote"

cmd=(
  /usr/bin/python3
  -m codex_session_viewer
  daemon
)

agent_dev_reload="${CODEX_VIEWER_AGENT_DEV_RELOAD:-${CODEX_VIEWER_DEV_RELOAD:-0}}"
agent_dev_reload_interval="${CODEX_VIEWER_AGENT_DEV_RELOAD_INTERVAL:-${CODEX_VIEWER_DEV_RELOAD_INTERVAL:-1}}"

if [[ "${agent_dev_reload}" == "1" ]]; then
  # Development mode restarts the daemon when agent-relevant code or local env changes.
  source "${PROJECT_ROOT}/scripts/lib-dev-reload.sh"
  CODEX_VIEWER_DEV_RELOAD_INTERVAL="${agent_dev_reload_interval}" run_with_dev_reload \
    "${PROJECT_ROOT}" \
    "${PROJECT_ROOT}/codex_session_viewer/__init__.py" \
    "${PROJECT_ROOT}/codex_session_viewer/__main__.py" \
    "${PROJECT_ROOT}/codex_session_viewer/commands.py" \
    "${PROJECT_ROOT}/codex_session_viewer/config.py" \
    "${PROJECT_ROOT}/codex_session_viewer/db.py" \
    "${PROJECT_ROOT}/codex_session_viewer/git_utils.py" \
    "${PROJECT_ROOT}/codex_session_viewer/importer.py" \
    "${PROJECT_ROOT}/codex_session_viewer/remote_sync.py" \
    "${PROJECT_ROOT}/codex_session_viewer/runtime.py" \
    "${PROJECT_ROOT}/scripts/lib-dev-reload.sh" \
    "${PROJECT_ROOT}/scripts/lib-env.sh" \
    "${PROJECT_ROOT}/scripts/start-agent-daemon.sh" \
    "${PROJECT_ROOT}/.env" \
    "${PROJECT_ROOT}/.env.local" \
    "${PROJECT_ROOT}/.env.development" \
    "${PROJECT_ROOT}/.env.development.local" \
    "${PROJECT_ROOT}/requirements.txt" \
    -- \
    "${cmd[@]}"
  exit $?
fi

agent_restart_delay="${CODEX_VIEWER_AGENT_RESTART_DELAY:-5}"
agent_restart_max_delay="${CODEX_VIEWER_AGENT_RESTART_MAX_DELAY:-60}"
child_pid=""
shutting_down=0

agent_log() {
  printf '[agent-daemon] %s\n' "$*"
}

forward_shutdown() {
  shutting_down=1
  if [[ -n "${child_pid}" ]] && kill -0 "${child_pid}" 2>/dev/null; then
    kill -TERM "${child_pid}" 2>/dev/null || true
  fi
}

trap forward_shutdown INT TERM

while true; do
  "${cmd[@]}" &
  child_pid="$!"
  agent_log "started pid=${child_pid}"

  if wait "${child_pid}"; then
    status=0
  else
    status=$?
  fi
  child_pid=""

  if ((shutting_down)); then
    exit 0
  fi

  if [[ "${status}" == "0" ]]; then
    exit 0
  fi

  if [[ "${status}" == "75" ]]; then
    agent_log "daemon requested restart"
    sleep 1
    continue
  fi

  agent_log "daemon exited with status=${status}; retrying in ${agent_restart_delay}s"
  sleep "${agent_restart_delay}"
  if [[ "${agent_restart_delay}" -lt "${agent_restart_max_delay}" ]]; then
    agent_restart_delay=$(( agent_restart_delay * 2 ))
    if [[ "${agent_restart_delay}" -gt "${agent_restart_max_delay}" ]]; then
      agent_restart_delay="${agent_restart_max_delay}"
    fi
  fi
done
