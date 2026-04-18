#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source "${PROJECT_ROOT}/scripts/lib-env.sh"
codex_load_env "${PROJECT_ROOT}"

export PYTHONPATH="${PROJECT_ROOT}/.deps${PYTHONPATH:+:${PYTHONPATH}}"

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

exec "${cmd[@]}"
