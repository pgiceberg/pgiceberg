#!/usr/bin/env bash
set -euo pipefail

cache_result() {
  case "${1:-}" in
    true)
      printf "exact hit"
      ;;
    false)
      printf "partial hit"
      ;;
    "")
      printf "miss"
      ;;
    *)
      printf "%s" "$1"
      ;;
  esac
}

context="${CACHE_CONTEXT:-${RUNNER_OS:-unknown} / PostgreSQL ${PG_VERSION:-unknown}}"

{
  echo "### Dependency cache restore"
  echo
  echo "| Cache | Scope | Result |"
  echo "| --- | --- | --- |"
  echo "| pgenv PostgreSQL | ${context} | $(cache_result "${PGENV_CACHE_HIT:-}") |"
  echo "| CMake dependencies | ${context} | $(cache_result "${CMAKE_DEPS_CACHE_HIT:-}") |"
} >> "${GITHUB_STEP_SUMMARY}"
