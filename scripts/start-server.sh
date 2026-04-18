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
  codex_session_viewer
  serve
  --no-sync
)

if [[ "${CODEX_VIEWER_DEV_RELOAD:-0}" == "1" ]]; then
  # Development mode restarts the child process on Python, template, and asset changes.
  source "${PROJECT_ROOT}/scripts/lib-dev-reload.sh"
  run_with_dev_reload \
    "${PROJECT_ROOT}" \
    "${PROJECT_ROOT}/codex_session_viewer" \
    "${PROJECT_ROOT}/codex_session_viewer/templates" \
    "${PROJECT_ROOT}/codex_session_viewer/static/app.css" \
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
