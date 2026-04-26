#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source "${PROJECT_ROOT}/scripts/lib-env.sh"
codex_load_env "${PROJECT_ROOT}"

export PYTHONPATH="${PROJECT_ROOT}/.deps${PYTHONPATH:+:${PYTHONPATH}}"

cmd=(
  /usr/bin/python3
  -m
  agent_operations_viewer
  serve
  --no-sync
  "$@"
)

server_dev_reload="${CODEX_VIEWER_SERVER_DEV_RELOAD:-0}"
server_dev_reload_interval="${CODEX_VIEWER_SERVER_DEV_RELOAD_INTERVAL:-1}"

if [[ "${server_dev_reload}" == "1" ]]; then
  # Development mode restarts the child process on Python, template, and asset changes.
  source "${PROJECT_ROOT}/scripts/lib-dev-reload.sh"
  run_with_dev_reload \
    "${PROJECT_ROOT}" \
    "${server_dev_reload_interval}" \
    "${PROJECT_ROOT}/agent_operations_viewer" \
    "${PROJECT_ROOT}/agent_operations_viewer/templates" \
    "${PROJECT_ROOT}/agent_operations_viewer/static/app.css" \
    "${PROJECT_ROOT}/src" \
    "${PROJECT_ROOT}/.env" \
    "${PROJECT_ROOT}/.env.local" \
    "${PROJECT_ROOT}/.env.development" \
    "${PROJECT_ROOT}/.env.development.local" \
    "${PROJECT_ROOT}/requirements.txt" \
    "${PROJECT_ROOT}/package.json" \
    "${PROJECT_ROOT}/package-lock.json" \
    "${PROJECT_ROOT}/tailwind.config.js" \
    -- \
    "${cmd[@]}"
  exit $?
fi

exec "${cmd[@]}"
