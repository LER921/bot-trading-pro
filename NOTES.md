# Notes

## Arbitrages techniques V2

- La priorite reste un runtime live sobre, testable et robuste, pas la sophistication strategique.
- Binance Spot est cable avec deux chemins complementaires:
  - WebSocket prioritaire pour le live event-driven
  - REST pour bootstrap, health sampling, resync et reconciliation
- Le market stream combine `depth@100ms` et `trade`.
- Le user stream est traite comme une source temps reel pour:
  - execution reports
  - fills
  - balance/account updates

## Arbitrage Binance user stream

- La documentation Spot Binance actuelle met en avant `userDataStream.subscribe.signature` via WebSocket API.
- L'implementation suit donc ce modele signe, plutot qu'un cycle legacy `listenKey` + keepalive.
- Le cycle reel devient:
  - `connect`
  - `subscribe.signature`
  - `consume events`
  - `reconnect/resubscribe` si la session degrade
- Le fallback REST garde la coherence compte/open orders pendant les deconnexions ou les resyncs.

## Pourquoi ce design

- Le runtime devient reactif en live sans perdre le filet de securite REST.
- Les deltas market sont appliques localement tant que la sequence reste coherente.
- Un trou de sequence force un resync propre par snapshot REST au lieu de continuer sur un book potentiellement faux.
- Les execution reports mettent a jour l'etat local de l'execution sans attendre un polling.

## Limites volontairement conservees

- Pas encore de pricing avance de market making.
- Pas encore de logique scalp riche.
- Pas encore de persistance durable SQLite/Postgres; le storage courant reste memoire.
- Pas encore de traitement exhaustif de toutes les variantes de payload Binance.
- Les hooks `drawdown guard` et `reject-rate guard` sont prepares mais pas encore completes.
- `reduce-only` spot reste une regle interne du bot, pas un flag natif Binance Spot.
