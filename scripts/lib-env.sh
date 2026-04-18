#!/usr/bin/env bash
set -euo pipefail

_codex_trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

_codex_load_env_file() {
  local path="$1"
  [[ -f "$path" ]] || return 0

  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(_codex_trim "$line")"
    [[ -n "$line" ]] || continue
    [[ "$line" == \#* ]] && continue
    [[ "$line" == export\ * ]] && line="${line#export }"
    [[ "$line" == *=* ]] || continue

    key="$(_codex_trim "${line%%=*}")"
    value="$(_codex_trim "${line#*=}")"
    [[ -n "$key" ]] || continue

    if [[ -n "${!key+x}" ]]; then
      continue
    fi

    if [[ ${#value} -ge 2 && "${value:0:1}" == "'" && "${value: -1}" == "'" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ ${#value} -ge 2 && "${value:0:1}" == '"' && "${value: -1}" == '"' ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == *" #"* ]]; then
      value="${value%% \#*}"
      value="$(_codex_trim "$value")"
    fi

    export "${key}=${value}"
  done < "$path"
}

codex_load_env() {
  local project_root="$1"
  local environment_name=""

  _codex_load_env_file "${project_root}/.env"
  environment_name="${CODEX_VIEWER_ENV:-}"
  if [[ -n "$environment_name" ]]; then
    _codex_load_env_file "${project_root}/.env.${environment_name}"
  fi
  _codex_load_env_file "${project_root}/.env.local"
  if [[ -n "$environment_name" ]]; then
    _codex_load_env_file "${project_root}/.env.${environment_name}.local"
  fi
}
