#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REQUIRE_BUILT=0

if [[ "${1:-}" == "--require-built" ]]; then
  REQUIRE_BUILT=1
fi

fail() {
  echo "[preflight] ERROR: $*" >&2
  exit 1
}

check_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing file: $path"
}

check_executable() {
  local path="$1"
  [[ -x "$path" ]] || fail "missing executable: $path"
}

check_env_value() {
  local key="$1"
  local value
  value="$(grep -E "^${key}=" "$ROOT_DIR/.env" | head -n 1 | cut -d= -f2- || true)"
  [[ -n "$value" ]] || fail "missing $key in .env"
  [[ "$value" != "replace-me" ]] || fail "$key still set to placeholder value"
}

check_file "$ROOT_DIR/.env"
check_file "$ROOT_DIR/config/base.toml"
check_file "$ROOT_DIR/config/live.toml"
check_file "$ROOT_DIR/config/pairs.toml"
check_file "$ROOT_DIR/config/risk.toml"
check_file "$ROOT_DIR/config/calibration.toml"
check_file "$ROOT_DIR/config/dashboard.toml"

command -v tmux >/dev/null 2>&1 || fail "tmux is not installed"
command -v curl >/dev/null 2>&1 || fail "curl is not installed"
command -v sqlite3 >/dev/null 2>&1 || fail "sqlite3 is not installed"

check_env_value "BINANCE_API_KEY"
check_env_value "BINANCE_API_SECRET"

mkdir -p "$ROOT_DIR/var/log" "$ROOT_DIR/var/state"

if [[ "$REQUIRE_BUILT" -eq 1 ]]; then
  check_executable "$ROOT_DIR/target/release/traderd"
  check_executable "$ROOT_DIR/target/release/dashboardd"
fi

echo "[preflight] OK root=$ROOT_DIR"
echo "[preflight] OK env=.env"
echo "[preflight] OK config files present"
echo "[preflight] OK var/log and var/state ready"
if [[ "$REQUIRE_BUILT" -eq 1 ]]; then
  echo "[preflight] OK release binaries present"
fi
