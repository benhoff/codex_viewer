#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source "${PROJECT_ROOT}/scripts/lib-env.sh"
codex_load_env "${PROJECT_ROOT}"

usage() {
  cat <<'EOF'
usage: ./scripts/reset-password.sh [options] USERNAME [NEW_PASSWORD]

Reset a local password-authenticated user's password using the app's built-in
hashing code.

Arguments:
  USERNAME              Local username to update
  NEW_PASSWORD          Optional. If omitted, the script prompts securely.

Options:
  --db PATH             Explicit SQLite database path
  --data-dir PATH       Override CODEX_VIEWER_DATA_DIR before resolving config
  -h, --help            Show this help text

Examples:
  ./scripts/reset-password.sh admin
  ./scripts/reset-password.sh admin 'NewPassword123!'
  ./scripts/reset-password.sh --db /opt/agent_operations_viewer_buddy/data/agent_operations_viewer_sessions.sqlite3 admin
EOF
}

log() {
  printf '[reset-password] %s\n' "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

db_path=""
data_dir=""
username=""
new_password=""

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
      break
      ;;
  esac
done

if (($# < 1 || $# > 2)); then
  usage >&2
  exit 1
fi

username="$1"
if (($# == 2)); then
  new_password="$2"
fi

if [[ -n "$data_dir" ]]; then
  export CODEX_VIEWER_DATA_DIR="$data_dir"
fi

if [[ -n "$db_path" ]]; then
  export CODEX_VIEWER_DB="$db_path"
fi

if [[ -z "$new_password" ]]; then
  read -r -s -p "New password: " new_password
  printf '\n'
  read -r -s -p "Confirm new password: " confirm_password
  printf '\n'
  if [[ "$new_password" != "$confirm_password" ]]; then
    printf 'passwords do not match\n' >&2
    exit 1
  fi
  unset confirm_password
fi

require_command python3

if [[ -d "${PROJECT_ROOT}/.deps" ]]; then
  export PYTHONPATH="${PROJECT_ROOT}/.deps${PYTHONPATH:+:${PYTHONPATH}}"
fi
export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

export CODEX_VIEWER_RESET_USERNAME="$username"
export CODEX_VIEWER_RESET_PASSWORD="$new_password"
export CODEX_VIEWER_PROJECT_ROOT="$PROJECT_ROOT"

python3 - <<'PY'
import os
from pathlib import Path

from agent_operations_viewer.config import Settings
from agent_operations_viewer.db import connect, write_transaction
from agent_operations_viewer.local_auth import fetch_user_by_username, update_user_password

project_root = Path(os.environ["CODEX_VIEWER_PROJECT_ROOT"]).resolve()
username = os.environ["CODEX_VIEWER_RESET_USERNAME"]
new_password = os.environ["CODEX_VIEWER_RESET_PASSWORD"]

settings = Settings.from_env(project_root)
db_path = settings.database_path
if not db_path.exists():
    raise SystemExit(f"database not found: {db_path}")

with connect(db_path) as connection:
    user = fetch_user_by_username(connection, username)
    if user is None:
        raise SystemExit(f"user not found: {username}")
    auth_source = str(user["auth_source"] or "password").strip() or "password"
    if auth_source != "password":
        raise SystemExit(
            f"user {username} is not a local password account (auth_source={auth_source})"
        )
    with write_transaction(connection):
        try:
            update_user_password(connection, str(user["id"]), new_password)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

print(f"reset password for {username} in {db_path}")
PY

unset CODEX_VIEWER_RESET_USERNAME
unset CODEX_VIEWER_RESET_PASSWORD
unset CODEX_VIEWER_PROJECT_ROOT

log "password updated for ${username}"
