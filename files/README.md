# Pipeline Reporting — étapes de mise en place

## Architecture

```
events_local (CH) ──► pipeline/ajout_reporting.py ──► reporting (CH, grain événement)
campaigns/payout (PG) ──► pipeline/update_reporting_focus.py ──► reporting_focus (CH, grain id_routers)

reporting_focus ──► [Dictionary reporting_focus_dict, en RAM, refresh auto]
reporting + dictGet(...) ──► vue reporting_enriched
reporting_enriched ──► api/main.py (FastAPI, lecture seule, zéro JOIN)
```

## 1. Créer le schéma ClickHouse

```bash
clickhouse-client --multiquery < sql/01_ddl.sql
```

Vérifier que le dictionary est bien chargé :

```sql
SELECT name, status, element_count, bytes_allocated
FROM system.dictionaries
WHERE name = 'reporting_focus_dict';
```

## 2. Alimenter reporting_focus (agrégats PG)

À lancer en premier, ou en parallèle du pipeline événementiel — les deux
sont indépendants.

```bash
python pipeline/update_reporting_focus.py
```

Adapter `date_start`/`date_end` selon la fenêtre voulue (peut couvrir
toute la période, ou être relancé en incrémental — `ReplacingMergeTree`
gère les mises à jour par `id_routers` sans doublon).

À planifier en cron (ex: toutes les nuits) pour tenir `reporting_focus`
à jour ; le dictionary se rafraîchit tout seul dans les 5-10 min qui
suivent (`LIFETIME`).

## 3. Alimenter reporting (événements)

```bash
python pipeline/ajout_reporting.py
```

Tourne par fenêtres de 3 jours sur toute la période (`iter_months`),
libère la RAM Python après chaque batch (`malloc_trim`).

## 4. Lancer l'API

```bash
pip install fastapi uvicorn clickhouse-connect
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

Endpoints :
- `GET /reporting?date_start=2025-01-01&date_end=2025-01-31` — detail événementiel enrichi
- `GET /reporting/summary?date_start=...&date_end=...&group_by=brand` — agrégats sans sur-comptage
- `GET /health`

## Points de vigilance

- **Ne jamais faire `SUM(ca)` directement sur `reporting_enriched`** sans
  regrouper par `id_routers` d'abord — `ca` est répété sur chaque
  événement du même routeur. Utiliser le pattern `any(ca)` groupé par
  `id_routers` comme dans `/reporting/summary`, ou passer par cette route.
- **`reporting_focus` doit rester petite** (une ligne par `id_routers`,
  pas par événement) — c'est ce qui permet au dictionary de tenir en RAM
  sans coût.
- Adapter les paramètres de connexion (`host`, `user`, `password`, `db`)
  dans `api/main.py` et dans le `SOURCE(CLICKHOUSE(...))` du dictionary
  si l'API et ClickHouse ne sont pas sur le même hôte que le pipeline.
