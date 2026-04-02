# Notes

## Arbitrages techniques

- Le runtime reste volontairement sobre: pas de ML, pas de microservices, pas de sophistication gratuite.
- Binance Spot est traite en WS-first:
  - market WS pour `depth` + `trade`
  - user WS pour execution reports, fills et balances
  - REST pour bootstrap, sampling health, resync et fallback
- Les features et le regime detector sont volontairement deterministes, explicables et testables.

## Pourquoi certains choix ont ete durcis

- Le book local refuse les deltas incoherents et force un resync au lieu de continuer sur un carnet potentiellement faux.
- Le bootstrap seed maintenant une vraie fenetre de trades recents pour que les features ne demarrent pas dans un etat artificiellement vide.
- Le score de toxicite a ete rendu moins brutal qu'une simple somme saturee, afin d'eviter de couper le market making a la moindre asymetrie de flux.
- La sante globale continue de tenir compte des WS meme en fallback: le fallback permet de survivre, pas de masquer une degradation structurelle.

## Ce qui est reellement en place

- runtime live event-driven avec market/user WS prioritaires
- orderbook local avec gap detection, resync et freshness
- moteur de features non stubs
- regime detector explicable
- market making avec filtre d'edge net apres couts
- scalping secondaire avec filtres stricts
- risk manager branche sur sante, inventaire, drawdown, exposure et reject rate
- execution stats, slippage, fill ratio et cancel-on-stale
- PnL realise / non realise / journalier
- persistance SQLite WAL des evenements critiques
- telemetry structuree pour sante, regime, execution et PnL

## Limites restantes

- le pricing market making reste volontairement simple; il est robuste mais pas encore optimise finement pour la capture d'edge
- le scalping reste conservateur et ne couvre pas encore de scenarios plus riches de breakout/reversal
- la telemetry est exposee en logs structures; il manque encore une exposition dashboard/API plus riche et des compteurs Prometheus explicites
- le storage SQLite couvre deja l'audit critique, mais pas encore tous les objets secondaires ni les migrations versionnees
- la couverture de tests est bonne sur les chemins critiques, mais il reste de la marge sur les cas Binance rares et les incidents long-run
