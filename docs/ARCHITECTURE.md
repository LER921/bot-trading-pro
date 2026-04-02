# Architecture V1

## Principes

- Binance Spot first, pas d'abstraction multi-exchange prematuree.
- Pipeline unique autorise: `signal -> risk -> execution -> exchange`.
- Les strategies n'envoient jamais d'ordres directement.
- Dashboard read-mostly: toute commande operateur passe par `control_plane`, puis `risk`, jamais directement vers `execution`.
- Robustesse live 24/7 prioritaire sur sophistication.

## Arborescence

```text
apps/
  traderd/        daemon principal et orchestration du runtime
  dashboardd/     API/UI read-mostly et observabilite
  ops-cli/        commandes d'exploitation et incidents

crates/
  common/                 utilitaires transverses
  domain/                 types metier centraux
  config_loader/          chargement et validation de la configuration
  exchange_binance_spot/  integration Binance Spot V1
  market_data/            ingestion WS/REST, carnet, trades, fraicheur
  features/               microprice, imbalance, vol, momentum, toxicity
  regime/                 classification execution du marche
  strategy_mm/            market making simple et prudent
  strategy_scalp/         scalping simple et filtre
  strategy_coordinator/   arbitrage entre strategies
  risk/                   risk manager strict
  execution/              ordres, etats, reconciliation, idempotence
  portfolio/              balances, inventaire, budgets
  accounting/             journal fills et base PnL
  storage/                persistance audit et reprise
  telemetry/              logs, metriques, alertes
  control_plane/          pause, reduced, risk-off, shutdown
```

## Etats du runtime

- `BOOTSTRAP`
- `RECONCILING`
- `READY`
- `TRADING`
- `REDUCED`
- `PAUSED`
- `RISK_OFF`
- `SHUTDOWN`

Le runtime `traderd` utilise une machine d'etat explicite. Les transitions sont validees et journalisees.

## Bootstrap et reconciliation

Le bootstrap securise fait:

1. chargement config
2. initialisation telemetry
3. ping REST et sync horloge exchange
4. recuperation account snapshot
5. recuperation open orders
6. recuperation fills recents
7. reconstruction minimale portfolio/accounting
8. chargement initial orderbook/trades
9. passage a `RECONCILING`
10. `cancel-all` optionnel sur les open orders portes
11. reconciliation des open orders
12. passage a `READY` seulement si l'etat est coherent

Si l'etat n'est pas coherent, le runtime passe en `RISK_OFF`.

## Sante systeme

Le suivi est separe pour:

- `market_ws_health`
- `user_ws_health`
- `REST health`
- `clock drift`
- `stale market data`
- `stale account/fill events`
- `last_market_event_age`
- `last_user_event_age`
- `fallback_active`
- `reconnect counters`

Un `overall_state` derive synthese ces composantes. Quand `fallback_active=true`, les WS ne sont plus la condition bloquante du `overall_state`; la sante se recompose alors a partir du chemin REST + clock + freshness locale. Le risk manager bloque si la sante n'est pas `HEALTHY`.

## Flux de donnees runtime

1. `exchange_binance_spot` alimente un `market stream` WebSocket Binance combine:
   - `depth@100ms`
   - `trade`
2. `exchange_binance_spot` alimente un `user data stream` WebSocket Binance signe
3. `market_data` maintient le carnet local event-driven, le dernier trade et les marqueurs de resync
4. `features` calcule les signaux d'execution a partir du snapshot local
5. `regime` classe le contexte en etats:
   - `RANGE`
   - `TREND_UP`
   - `TREND_DOWN`
   - `HIGH_VOLATILITY`
   - `DEAD_MARKET`
   - `NO_TRADE`
   - `RISK_OFF`
6. `strategy_coordinator` choisit `market making`, `scalping` ou `standby`
7. les strategies emettent des `TradeIntent`
8. `risk` approuve, bloque ou force `reduce-only`
9. `execution` construit les ordres Binance Spot, suit les open orders et reconcilie
10. `portfolio`, `accounting`, `storage` et `telemetry` sont mis a jour

Le chemin live prioritaire est donc:

`market WS -> market_data -> features -> regime -> strategy_coordinator -> strategies -> TradeIntent -> risk -> execution`

`user WS -> portfolio / execution / accounting / health`

Le chemin REST reste actif pour:

- bootstrap
- ping et sync clock
- snapshots depth/account
- reconciliation des open orders
- fallback quand un stream degrade ou devient stale

## Politique d'inventaire spot

- `max inventory` par symbole
- `max open orders` par symbole
- `max notional` par intent
- `quote skew` selon inventaire a venir
- neutralisation progressive
- `reduce-only` si le desequilibre devient trop fort

Sur Binance Spot, `reduce-only` est une regle interne du bot, pas un flag exchange natif.

## Etat actuel de la boucle live

- La boucle `traderd` est maintenant cablee de bout en bout.
- Les strategies restent simples et lisibles par design.
- Les WebSocket Binance `market` et `user` sont la source live prioritaire du runtime.
- Le runtime draine les evenements WS a chaque cycle, applique les mises a jour locales, puis n'active le polling REST qu'en fallback ou en reconciliation.
- Les deltas depth qui presentent un trou de sequence forcent un resync par snapshot REST.
- Les execution reports du user stream mettent a jour:
  - `execution`
  - `portfolio`
  - `accounting`
  - `health`
- Le runtime passe en `REDUCED` quand le fallback REST devient actif et en `RISK_OFF` si la sante globale n'est plus exploitable.
