#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source "${PROJECT_ROOT}/scripts/lib-env.sh"
codex_load_env "${PROJECT_ROOT}"

usage() {
  cat <<'EOF'
usage: ./scripts/reset-setup.sh [options]

Reset setup/onboarding state so the setup wizard can be exercised again.

By default this script:
  - resets onboarding progress markers
  - deletes sync API tokens
  - deletes remote agent records and alert state
  - deletes imported sessions and orphaned stored raw artifacts
  - keeps existing users/admin accounts

Use --full-bootstrap to remove users as well, which rewinds setup all the way
back to the "First Admin Account" step.

Options:
  --db PATH             Explicit SQLite database path
  --data-dir PATH       Override CODEX_VIEWER_DATA_DIR before resolving config
  --keep-tokens         Preserve API tokens
  --keep-agents         Preserve remote agent and alert records
  --keep-sessions       Preserve imported sessions and stored artifacts
  --full-bootstrap      Delete all users and clear auth bootstrap state
  -y, --yes             Skip the confirmation prompt
  -h, --help            Show this help text

Examples:
  ./scripts/reset-setup.sh
  ./scripts/reset-setup.sh --full-bootstrap
  ./scripts/reset-setup.sh --db /opt/agent_operations_viewer_buddy/data/agent_operations_viewer_sessions.sqlite3 --yes
EOF
}

log() {
  printf '[reset-setup] %s\n' "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

db_path=""
data_dir=""
reset_tokens=1
reset_agents=1
reset_sessions=1
full_bootstrap=0
assume_yes=0

while (($#)); do
  case "$1" in
    --db)
      db_path="${2:?missing value for --db}"
      shift 2
      ;;
    --data-dir)
      data_dir="${2:?missing value for --data-dir}"
      shift 2
      ;;
    --keep-tokens)
      reset_tokens=0
      shift
      ;;
    --keep-agents)
      reset_agents=0
      shift
      ;;
    --keep-sessions)
      reset_sessions=0
      shift
      ;;
    --full-bootstrap)
      full_bootstrap=1
      shift
      ;;
    -y|--yes)
      assume_yes=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      printf 'unknown option: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
    *)
      printf 'unexpected argument: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if (($#)); then
  printf 'unexpected argument: %s\n' "$1" >&2
  usage >&2
  exit 1
fi

if [[ -n "$data_dir" ]]; then
  export CODEX_VIEWER_DATA_DIR="$data_dir"
fi

if [[ -n "$db_path" ]]; then
  export CODEX_VIEWER_DB="$db_path"
  if [[ -z "$data_dir" ]]; then
    export CODEX_VIEWER_DATA_DIR="$(cd -- "$(dirname -- "$db_path")" && pwd)"
  fi
fi

if (( ! assume_yes )); then
  printf 'This will reset setup state'
  if (( full_bootstrap )); then
    printf ' and delete all users'
  fi
  printf '.\n'
  (( reset_tokens )) && printf '  - delete API tokens\n'
  (( reset_agents )) && printf '  - delete remote agents and alert state\n'
  (( reset_sessions )) && printf '  - delete imported sessions and stored raw artifacts\n'
  printf '  - reset onboarding progress markers\n'
  read -r -p 'Continue? [y/N] ' response
  case "${response}" in
    y|Y|yes|YES)
      ;;
    *)
      printf 'aborted\n' >&2
      exit 1
      ;;
  esac
fi

require_command python3

if [[ -d "${PROJECT_ROOT}/.deps" ]]; then
  export PYTHONPATH="${PROJECT_ROOT}/.deps${PYTHONPATH:+:${PYTHONPATH}}"
fi
export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

export CODEX_VIEWER_PROJECT_ROOT="$PROJECT_ROOT"
export CODEX_VIEWER_RESET_SETUP_TOKENS="$reset_tokens"
export CODEX_VIEWER_RESET_SETUP_AGENTS="$reset_agents"
export CODEX_VIEWER_RESET_SETUP_SESSIONS="$reset_sessions"
export CODEX_VIEWER_RESET_SETUP_FULL_BOOTSTRAP="$full_bootstrap"

python3 - <<'PY'
import json
import os
from pathlib import Path

from agent_operations_viewer.config import Settings
from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.setup_reset import (
    prune_empty_artifact_dirs,
    remove_artifact_files,
    reset_setup_state,
)

project_root = Path(os.environ["CODEX_VIEWER_PROJECT_ROOT"]).resolve()
reset_tokens = os.environ["CODEX_VIEWER_RESET_SETUP_TOKENS"] == "1"
reset_agents = os.environ["CODEX_VIEWER_RESET_SETUP_AGENTS"] == "1"
reset_sessions = os.environ["CODEX_VIEWER_RESET_SETUP_SESSIONS"] == "1"
full_bootstrap = os.environ["CODEX_VIEWER_RESET_SETUP_FULL_BOOTSTRAP"] == "1"

settings = Settings.from_env(project_root)
db_path = settings.database_path
if not db_path.exists():
    raise SystemExit(f"database not found: {db_path}")

with connect(db_path) as connection:
    with write_transaction(connection):
        result = reset_setup_state(
            connection,
            settings,
            reset_tokens=reset_tokens,
            reset_remote_agents=reset_agents,
            reset_sessions=reset_sessions,
            full_bootstrap=full_bootstrap,
        )

artifact_files_removed = remove_artifact_files(result.artifact_paths)
prune_empty_artifact_dirs(settings)

print(
    json.dumps(
        {
            "database_path": str(db_path),
            "users_removed": result.users_removed,
            "tokens_removed": result.tokens_removed,
            "remote_agents_removed": result.remote_agents_removed,
            "alert_incidents_removed": result.alert_incidents_removed,
            "alert_deliveries_removed": result.alert_deliveries_removed,
            "sessions_removed": result.sessions_removed,
            "session_artifacts_removed": result.session_artifacts_removed,
            "artifact_files_removed": artifact_files_removed,
            "full_bootstrap": full_bootstrap,
        },
        sort_keys=True,
    )
)
PY

unset CODEX_VIEWER_PROJECT_ROOT
unset CODEX_VIEWER_RESET_SETUP_TOKENS
unset CODEX_VIEWER_RESET_SETUP_AGENTS
unset CODEX_VIEWER_RESET_SETUP_SESSIONS
unset CODEX_VIEWER_RESET_SETUP_FULL_BOOTSTRAP

log "setup state reset"
