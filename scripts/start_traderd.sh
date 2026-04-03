#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION_NAME="${TMUX_TRADERD_SESSION:-bot_live}"

bash "$ROOT_DIR/scripts/preflight_live.sh" --require-built

tmux has-session -t "$SESSION_NAME" 2>/dev/null && tmux kill-session -t "$SESSION_NAME" || true
pkill -TERM -f "$ROOT_DIR/target/release/traderd" 2>/dev/null || true
sleep 1

tmux new-session -d -s "$SESSION_NAME" "bash -lc 'cd \"$ROOT_DIR\" && set -a && source .env && set +a && mkdir -p var/log var/state && exec ./target/release/traderd >> var/log/traderd-live.log 2>&1'"

echo "[start_traderd] session=$SESSION_NAME"
echo "[start_traderd] log=$ROOT_DIR/var/log/traderd-live.log"
