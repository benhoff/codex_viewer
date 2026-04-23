#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_REPO="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

TARGET_ROOT="/opt/codex_viewer_buddy"
SERVICE_NAME="codex-session-viewer-buddy"
RUN_USER="hoff"
RUN_GROUP="hoff"
PORT="8002"
SERVER_URL="http://192.168.1.125:8002"
SOURCE_HOST="arch-buddy"
START_SERVICE=1

usage() {
  cat <<'EOF'
usage: sudo ./scripts/install-buddy-instance.sh [options]

Options:
  --target-root PATH   Install path for the isolated copy. Default: /opt/codex_viewer_buddy
  --user USER          systemd service user. Default: hoff
  --group GROUP        systemd service group. Default: hoff
  --port PORT          Listen port for the isolated viewer. Default: 8002
  --server-url URL     External base URL shown in setup. Default: http://192.168.1.125:8002
  --source-host NAME   Source host label for this instance. Default: arch-buddy
  --no-start           Install and enable the service, but do not start it
  -h, --help           Show this help text

The script intentionally installs only a separate viewer service. It does not
install a local agent daemon, so the new instance cannot accidentally ingest
your own local rollout tree.
EOF
}

log() {
  printf '[install-buddy-instance] %s\n' "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

while (($#)); do
  case "$1" in
    --target-root)
      TARGET_ROOT="${2:?missing value for --target-root}"
      shift 2
      ;;
    --user)
      RUN_USER="${2:?missing value for --user}"
      shift 2
      ;;
    --group)
      RUN_GROUP="${2:?missing value for --group}"
      shift 2
      ;;
    --port)
      PORT="${2:?missing value for --port}"
      shift 2
      ;;
    --server-url)
      SERVER_URL="${2:?missing value for --server-url}"
      shift 2
      ;;
    --source-host)
      SOURCE_HOST="${2:?missing value for --source-host}"
      shift 2
      ;;
    --no-start)
      START_SERVICE=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${EUID}" -ne 0 ]]; then
  printf 'run this script with sudo or as root\n' >&2
  exit 1
fi

require_command tar
require_command install
require_command sed
require_command systemctl

if ! id "${RUN_USER}" >/dev/null 2>&1; then
  printf 'unknown user: %s\n' "${RUN_USER}" >&2
  exit 1
fi

if ! getent group "${RUN_GROUP}" >/dev/null 2>&1; then
  printf 'unknown group: %s\n' "${RUN_GROUP}" >&2
  exit 1
fi

if [[ ! -d "${SOURCE_REPO}/.deps" ]]; then
  printf 'missing %s/.deps; bootstrap the source repo first\n' "${SOURCE_REPO}" >&2
  exit 1
fi

if [[ ! -f "${SOURCE_REPO}/codex_session_viewer/static/app.css" ]]; then
  printf 'missing built CSS at %s/codex_session_viewer/static/app.css\n' "${SOURCE_REPO}" >&2
  exit 1
fi

if command -v ss >/dev/null 2>&1; then
  if ss -H -ltn "( sport = :${PORT} )" | grep -q .; then
    printf 'port %s is already in use\n' "${PORT}" >&2
    exit 1
  fi
fi

TARGET_ENV="${TARGET_ROOT}/deploy/systemd/${SERVICE_NAME}.env"
TARGET_SERVICE_TEMPLATE="${TARGET_ROOT}/deploy/systemd/${SERVICE_NAME}.service"
INSTALLED_SERVICE="/etc/systemd/system/${SERVICE_NAME}.service"

log "creating target directories under ${TARGET_ROOT}"
install -d -o "${RUN_USER}" -g "${RUN_GROUP}" "${TARGET_ROOT}"
install -d -o "${RUN_USER}" -g "${RUN_GROUP}" "${TARGET_ROOT}/data"
install -d -o "${RUN_USER}" -g "${RUN_GROUP}" "${TARGET_ROOT}/session_roots"

log "copying application tree into ${TARGET_ROOT}"
tar -C "${SOURCE_REPO}" \
  --exclude='.git' \
  --exclude='data' \
  --exclude='.env' \
  --exclude='.env.*' \
  --exclude='node_modules' \
  -cf - . | tar -C "${TARGET_ROOT}" -xf -

chown -R "${RUN_USER}:${RUN_GROUP}" "${TARGET_ROOT}"

if [[ ! -f "${TARGET_ENV}" ]]; then
  printf 'expected env template is missing: %s\n' "${TARGET_ENV}" >&2
  exit 1
fi

if [[ ! -f "${TARGET_SERVICE_TEMPLATE}" ]]; then
  printf 'expected service template is missing: %s\n' "${TARGET_SERVICE_TEMPLATE}" >&2
  exit 1
fi

log "patching copied env and service files"
sed -i \
  -e "s|^CODEX_VIEWER_PORT=.*$|CODEX_VIEWER_PORT=${PORT}|" \
  -e "s|^CODEX_VIEWER_SERVER_URL=.*$|CODEX_VIEWER_SERVER_URL=${SERVER_URL}|" \
  -e "s|^CODEX_VIEWER_SOURCE_HOST=.*$|CODEX_VIEWER_SOURCE_HOST=${SOURCE_HOST}|" \
  -e "s|^CODEX_SESSION_ROOTS=.*$|CODEX_SESSION_ROOTS=${TARGET_ROOT}/session_roots|" \
  -e "s|^CODEX_VIEWER_DB=.*$|CODEX_VIEWER_DB=${TARGET_ROOT}/data/codex_sessions.sqlite3|" \
  -e "s|^CODEX_VIEWER_DATA_DIR=.*$|CODEX_VIEWER_DATA_DIR=${TARGET_ROOT}/data|" \
  "${TARGET_ENV}"

sed -i \
  -e "s|^User=.*$|User=${RUN_USER}|" \
  -e "s|^Group=.*$|Group=${RUN_GROUP}|" \
  -e "s|^WorkingDirectory=.*$|WorkingDirectory=${TARGET_ROOT}|" \
  -e "s|^EnvironmentFile=.*$|EnvironmentFile=-${TARGET_ENV}|" \
  -e "s|^ExecStart=.*$|ExecStart=${TARGET_ROOT}/scripts/start-server.sh|" \
  "${TARGET_SERVICE_TEMPLATE}"

log "installing systemd unit ${INSTALLED_SERVICE}"
install -m 644 "${TARGET_SERVICE_TEMPLATE}" "${INSTALLED_SERVICE}"

log "reloading systemd"
systemctl daemon-reload

if ((START_SERVICE)); then
  log "enabling and starting ${SERVICE_NAME}"
  systemctl enable --now "${SERVICE_NAME}"
else
  log "enabling ${SERVICE_NAME} without starting it"
  systemctl enable "${SERVICE_NAME}"
fi

if ((START_SERVICE)); then
  if systemctl is-active --quiet "${SERVICE_NAME}"; then
    log "service is active"
  else
    printf 'service did not become active: %s\n' "${SERVICE_NAME}" >&2
    systemctl status "${SERVICE_NAME}" --no-pager || true
    exit 1
  fi
fi

cat <<EOF

Installed isolated viewer instance:
  Service: ${SERVICE_NAME}
  Root: ${TARGET_ROOT}
  URL: ${SERVER_URL}
  Env: ${TARGET_ENV}

Next steps:
  1. Open ${SERVER_URL}/setup and create the first admin user for this instance.
  2. Create a managed API token in setup/settings for your buddy's remote machine.
  3. Put that token on the buddy machine with:
     CODEX_VIEWER_SYNC_MODE=remote
     CODEX_VIEWER_SERVER_URL=${SERVER_URL}
     CODEX_VIEWER_SYNC_API_TOKEN=<raw-token>
     CODEX_VIEWER_SOURCE_HOST=<buddy-machine-name>

Useful checks:
  systemctl status ${SERVICE_NAME} --no-pager
  journalctl -u ${SERVICE_NAME} -n 50 --no-pager
EOF
