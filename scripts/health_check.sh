#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="$ROOT_DIR/var/state/traderd.sqlite3"
TRADER_LOG="$ROOT_DIR/var/log/traderd-live.log"
DASHBOARD_LOG="$ROOT_DIR/var/log/dashboardd.log"

echo "[health_check] root=$ROOT_DIR"
echo "[health_check] tmux sessions:"
tmux ls 2>/dev/null || true

echo "[health_check] traderd process:"
pgrep -af "$ROOT_DIR/target/release/traderd" || true

echo "[health_check] dashboardd process:"
pgrep -af "$ROOT_DIR/target/release/dashboardd" || true

echo "[health_check] port 8080:"
ss -ltnp | grep 8080 || true

echo "[health_check] dashboard healthz:"
curl -fsS http://127.0.0.1:8080/healthz || true
echo

echo "[health_check] dashboard summary:"
curl -fsS http://127.0.0.1:8080/api/summary > /dev/null && echo "OK /api/summary" || true

if [[ -f "$DB_PATH" ]]; then
  echo "[health_check] sqlite runtime tail:"
  sqlite3 -header -column "$DB_PATH" "SELECT transitioned_at, to_state, reason FROM runtime_transitions ORDER BY id DESC LIMIT 5;" || true
else
  echo "[health_check] sqlite missing: $DB_PATH"
fi

if [[ -f "$TRADER_LOG" ]]; then
  echo "[health_check] traderd log tail:"
  tail -n 20 "$TRADER_LOG" || true
else
  echo "[health_check] traderd log missing: $TRADER_LOG"
fi

if [[ -f "$DASHBOARD_LOG" ]]; then
  echo "[health_check] dashboardd log tail:"
  tail -n 20 "$DASHBOARD_LOG" || true
else
  echo "[health_check] dashboardd log missing: $DASHBOARD_LOG"
fi
