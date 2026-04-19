#!/usr/bin/env bash
set -euo pipefail

dev_reload_log() {
  printf '[dev-reload] %s\n' "$*"
}

dev_reload_snapshot() {
  if (($# == 0)); then
    printf 'no-watch-paths\n'
    return 0
  fi

  {
    local path
    for path in "$@"; do
      if [[ -d "$path" ]]; then
        find "$path" \
          \( -type d \( -name '__pycache__' -o -name '.mypy_cache' -o -name '.pytest_cache' \) -prune \) -o \
          -type f -printf '%T@ %p\n'
      elif [[ -f "$path" ]]; then
        printf '%s %s\n' "$(stat -c '%Y' "$path")" "$path"
      fi
    done
  } | LC_ALL=C sort | sha256sum | awk '{print $1}'
}

run_with_dev_reload() {
  local project_root="$1"
  shift
  local interval="$1"
  shift

  local -a watch_paths=()
  while (($#)); do
    if [[ "$1" == "--" ]]; then
      shift
      break
    fi
    watch_paths+=("$1")
    shift
  done

  local -a cmd=("$@")
  local baseline current status
  local child_pid=""
  local shutting_down=0

  if ((${#cmd[@]} == 0)); then
    echo "run_with_dev_reload requires a command" >&2
    return 1
  fi

  forward_shutdown() {
    shutting_down=1
    if [[ -n "$child_pid" ]] && kill -0 "$child_pid" 2>/dev/null; then
      kill -TERM "$child_pid" 2>/dev/null || true
    fi
  }

  trap forward_shutdown INT TERM

  baseline="$(dev_reload_snapshot "${watch_paths[@]}")"
  dev_reload_log "watching ${project_root} for changes every ${interval}s"

  while true; do
    "${cmd[@]}" &
    child_pid="$!"
    dev_reload_log "started pid=${child_pid}"

    while kill -0 "$child_pid" 2>/dev/null; do
      sleep "$interval" || true

      if ((shutting_down)); then
        if wait "$child_pid"; then
          :
        else
          :
        fi
        return 0
      fi

      current="$(dev_reload_snapshot "${watch_paths[@]}")"
      if [[ "$current" != "$baseline" ]]; then
        dev_reload_log "change detected, restarting pid=${child_pid}"
        baseline="$current"
        kill -TERM "$child_pid" 2>/dev/null || true
        if wait "$child_pid"; then
          :
        else
          :
        fi
        child_pid=""
        break
      fi
    done

    if ((shutting_down)); then
      return 0
    fi

    if [[ -n "$child_pid" ]]; then
      if wait "$child_pid"; then
        status=0
      else
        status=$?
      fi
      child_pid=""
      if [[ "$status" == "75" ]]; then
        dev_reload_log "child requested restart"
        baseline="$(dev_reload_snapshot "${watch_paths[@]}")"
        continue
      fi
      return "$status"
    fi
  done
}
