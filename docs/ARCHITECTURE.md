# Architecture V2

## Principes

- Binance Spot first, pas d'abstraction multi-exchange prematuree.
- Pipeline unique autorise: `market/feature/signal -> risk -> execution -> exchange`.
- Les strategies n'envoient jamais d'ordres directement.
- Dashboard read-mostly: toute commande operateur doit passer par `control_plane`, puis `risk`, jamais directement vers `execution`.
- WebSocket prioritaire pour le live, REST uniquement pour bootstrap, reconciliation, health sampling et fallback.
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
  market_data/            ingestion WS/REST, carnet, trades, freshness
  features/               moteur de features microstructure
  regime/                 classification execution du marche
  strategy_mm/            market making net des couts, simple et prudent
  strategy_scalp/         scalping secondaire, filtre strict
  strategy_coordinator/   arbitrage entre strategies
  risk/                   risk manager strict
  execution/              ordres, etats, reconciliation, idempotence
  portfolio/              balances, inventaire, budgets
  accounting/             PnL, fees, journal fills
  storage/                persistance SQLite WAL et reprise
  telemetry/              logs et metriques structurees
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

Le runtime `traderd` utilise une machine d'etat explicite. Les transitions sont validees, journalisees et persistees.

## Bootstrap et reconciliation

Le bootstrap securise fait:

1. chargement config
2. initialisation telemetry
3. ping REST et sync horloge exchange
4. recuperation account snapshot
5. recuperation open orders
6. recuperation fills recents
7. reconstruction minimale portfolio/accounting
8. chargement initial orderbook + seed d'une fenetre de trades recents
9. passage a `RECONCILING`
10. `cancel-all` optionnel sur les open orders portes
11. reconciliation des open orders
12. ouverture des WebSocket market et user
13. passage a `READY`, puis `TRADING` ou `PAUSED` selon la config

Si l'etat n'est pas coherent, le runtime passe en `RISK_OFF`.

## Market data et sante live

Le market WebSocket est la source principale pour:

- `best bid/ask`
- orderbook local
- trade stream
- timestamps de marche

Le moteur `market_data` gere:

- snapshots depth REST
- deltas depth WS avec validation des sequences
- detection de gaps
- protection contre book incoherent ou croise
- resync snapshot propre quand le book est marque `needs_resync`
- freshness/staleness

Les marqueurs exposes par snapshot incluent:

- `last_event_latency_ms`
- `book_age_ms`
- `trade_flow_rate`
- `book_crossed`
- `ws_reconnect_counter`

Le suivi sante est separe pour:

- `market_ws`
- `user_ws`
- `rest`
- `clock_drift`
- `market_data`
- `account_events`
- `fallback_active`
- compteurs de reconnexion

Le trading est bloque par le risk manager si la sante globale n'est pas `HEALTHY`. Le runtime degrade vers `REDUCED` sur WS degrades et vers `RISK_OFF` sur blocs durs comme book stale, clock drift excessif ou REST indisponible.

## Feature engine

`crates/features` calcule des features non triviales, toutes testables:

- realized volatility en rolling window
- VWAP et distance au VWAP
- spread mean/std/zscore
- orderbook imbalance instantane et rolling
- microprice
- trade flow imbalance
- trade flow rate
- trade rate / tick rate
- momentum 1s / 5s / 15s
- liquidity score
- toxicity score simple
- volatility regime

Ces features sont calculees a partir du snapshot local `market_data` et servent directement au regime detector et aux strategies.

## Regime detector

Le regime detector prend les vraies features et retourne des decisions explicables:

- `RANGE`
- `TREND_UP`
- `TREND_DOWN`
- `HIGH_VOLATILITY`
- `DEAD_MARKET`
- `NO_TRADE`
- `RISK_OFF`

Chaque decision journalise sa raison a partir de la volatilite, du spread regime, du momentum, de l'imbalance, de la liquidite et de la sante systeme.

## Strategies

`strategy_mm` est la strategie principale:

- mid price oriente par `microprice`
- skew selon inventaire
- skew selon momentum
- widening selon volatilite
- filtre `edge_after_cost_bps > 0`
- zones `no-trade` si spread trop faible, toxicite trop forte ou liquidite trop faible
- `reduce-only` interne si l'inventaire est trop desequilibre

`strategy_scalp` reste secondaire et sobre:

- score d'entree sur momentum, trade flow, VWAP distance et liquidite
- filtres stricts contre le surtrading
- sorties simples via TP/SL logiques et `reduce-only`

Le `strategy_coordinator` favorise le market making et ne bascule vers le scalping que quand le regime et les features sont suffisamment convaincants.

## Risk, execution, PnL

Le risk manager applique notamment:

- blocage si runtime state n'autorise pas
- blocage si health degradee
- max inventory par symbole
- max open orders
- max notional par intent
- exposure par symbole
- exposure globale
- `reduce-only` si inventaire excessif
- max daily loss
- drawdown guard
- reject-rate guard

L'execution engine gere:

- idempotence basique
- cycle de vie minimal des ordres
- open orders locaux
- `cancel-all`
- reconcile
- cancel-on-stale
- slippage calcule
- fill ratio
- latence decision -> ordre
- stats d'execution agregees

`accounting` maintient:

- realized PnL
- unrealized PnL
- PnL net
- fees
- PnL journalier
- drawdown global et par symbole

## Storage et observabilite

`storage` persiste maintenant en SQLite WAL:

- transitions runtime
- decisions risk
- execution reports
- fills
- snapshots PnL

`telemetry` emet des journaux structures pour:

- health detaillee
- features
- regime courant
- decisions risk
- execution reports
- execution stats
- PnL live

## Flux de donnees runtime

```text
market WS -> market_data -> features -> regime -> strategy_coordinator
          -> strategy_mm / strategy_scalp -> TradeIntent -> risk -> execution -> Binance

user WS -> execution / portfolio / accounting / health

REST -> bootstrap / clock sync / reconcile / resync snapshot / fallback
```

Le chemin live prioritaire est donc event-driven, avec le polling REST comme filet de securite et mecanisme de coherence.
