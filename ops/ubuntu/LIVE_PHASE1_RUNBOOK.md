# Live Phase 1 Runbook

## Preflight

- `.env` present with real `BINANCE_API_KEY` and `BINANCE_API_SECRET`
- `config/` files present
- `target/release/traderd` and `target/release/dashboardd` built
- `var/log/` and `var/state/` exist
- `scripts/preflight_live.sh --require-built` returns `OK`

## Launch

```bash
bash scripts/stop_stack.sh
bash scripts/start_traderd.sh
bash scripts/start_dashboardd.sh
bash scripts/health_check.sh
```

## Immediate checks

- `tmux ls`
- `tail -f var/log/traderd-live.log`
- `tail -f var/log/dashboardd.log`
- `sqlite3 var/state/traderd.sqlite3 "SELECT transitioned_at, to_state, reason FROM runtime_transitions ORDER BY id DESC LIMIT 10;"`
- Dashboard URL: `http://<VPS_IP>:8080/`

## First 15-30 minutes

- runtime stabilizes in `Trading`
- `user_ws` and `account_events` recover to healthy
- some execution reports / fills appear without violent inventory drift
- no persistent `high_churn`
- no repeated `RiskOff` / `Reconciling`

## Stop

```bash
bash scripts/stop_stack.sh
```
