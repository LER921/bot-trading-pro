#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TRADERD_SESSION="${TMUX_TRADERD_SESSION:-bot_live}"
DASHBOARDD_SESSION="${TMUX_DASHBOARDD_SESSION:-bot_dash}"

tmux has-session -t "$TRADERD_SESSION" 2>/dev/null && tmux kill-session -t "$TRADERD_SESSION" || true
tmux has-session -t "$DASHBOARDD_SESSION" 2>/dev/null && tmux kill-session -t "$DASHBOARDD_SESSION" || true

pkill -TERM -f "$ROOT_DIR/target/release/traderd" 2>/dev/null || true
pkill -TERM -f "$ROOT_DIR/target/release/dashboardd" 2>/dev/null || true
sleep 2
pkill -KILL -f "$ROOT_DIR/target/release/traderd" 2>/dev/null || true
pkill -KILL -f "$ROOT_DIR/target/release/dashboardd" 2>/dev/null || true

echo "[stop_stack] traderd_session=$TRADERD_SESSION dashboardd_session=$DASHBOARDD_SESSION stopped"
