#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

export PYTHONPATH="${PROJECT_ROOT}/.deps${PYTHONPATH:+:${PYTHONPATH}}"

cmd=(
  /usr/bin/python3
  -m codex_session_viewer
  daemon
  --interval "${CODEX_VIEWER_SYNC_INTERVAL:-30}"
)

if [[ "${CODEX_VIEWER_DAEMON_REBUILD_ON_START:-0}" == "1" ]]; then
  cmd+=(--rebuild-on-start)
fi

if [[ "${CODEX_VIEWER_DEV_RELOAD:-0}" == "1" ]]; then
  # Development mode restarts the daemon when shared app or project config changes.
  source "${PROJECT_ROOT}/scripts/lib-dev-reload.sh"
  run_with_dev_reload \
    "${PROJECT_ROOT}" \
    "${PROJECT_ROOT}/codex_session_viewer" \
    "${PROJECT_ROOT}/scripts" \
    "${PROJECT_ROOT}/src" \
    "${PROJECT_ROOT}/requirements.txt" \
    "${PROJECT_ROOT}/package.json" \
    "${PROJECT_ROOT}/package-lock.json" \
    "${PROJECT_ROOT}/tailwind.config.js" \
    -- \
    "${cmd[@]}"
  exit $?
fi

exec "${cmd[@]}"
