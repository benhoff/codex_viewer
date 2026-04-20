#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

skip_css=0
while (($#)); do
  case "$1" in
    --skip-css)
      skip_css=1
      ;;
    *)
      printf 'usage: %s [--skip-css]\n' "${BASH_SOURCE[0]}" >&2
      exit 1
      ;;
  esac
  shift
done

bootstrap_log() {
  printf '[bootstrap-local] %s\n' "$*"
}

bootstrap_log "installing Python dependencies into .deps"
python3 -m pip install --upgrade --target "$PROJECT_ROOT/.deps" -r "$PROJECT_ROOT/requirements.txt"

if ((skip_css)); then
  bootstrap_log "skipping CSS build"
  exit 0
fi

if [[ -f "$PROJECT_ROOT/codex_session_viewer/static/app.css" ]]; then
  bootstrap_log "found prebuilt CSS; skipping Tailwind build"
  exit 0
fi

if ! command -v npm >/dev/null 2>&1; then
  bootstrap_log "codex_session_viewer/static/app.css is missing and npm is not installed"
  bootstrap_log "install Node.js and rerun this script, or use Docker instead"
  exit 1
fi

bootstrap_log "building CSS assets"
npm ci
npm run build:css
