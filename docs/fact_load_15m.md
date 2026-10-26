# Fait de charge à 15 minutes

`analytics.fact_load_15m` contient une ligne par instant UTC `PT15M` de
`staging.elia_load`. Le modèle dbt est une table reconstruite entièrement à
chaque `dbt run`. Les volumes cités plus bas sont ceux du contrôle daté du
19 octobre 2026, avant l'ingestion du 26 octobre.
Les jointures vers `dim_date` et `dim_time` sont `LEFT JOIN` : une clé absente
ne retire aucune ligne source et fait échouer les tests de clé et de relation.

## Temps et clés

`interval_start_utc` est le `TIMESTAMPTZ` source ; il identifie l'observation.
`date_key` (`YYYYMMDD`) et `time_key` (`HHMM`) viennent des dimensions après
conversion de l'instant en `Europe/Brussels`. `utc_offset_minutes` vaut 60 ou
120 et distingue les créneaux locaux répétés lors du retour à l'heure d'hiver.
Deux lignes peuvent partager `date_key` et `time_key`, jamais
`interval_start_utc`.

Le champ source s'appelle `Datetime`, sa résolution est `PT15M` et tous ses
instants stockés sont alignés sur les quarts d'heure. Il est interprété ici comme
le début de l'intervalle, conformément à la convention des jeux de données
PT15M d'Elia. La page de métadonnées ODS001 était indisponible lors de la
vérification : confirmer cette convention propre à ODS001 avant de publier une
énergie calculée à partir des MW.

Références : [jeu ODS001 d'Elia](https://opendata.elia.be/explore/dataset/ods001/)
et [métadonnées Elia d'un autre jeu PT15M](https://opendata.elia.be/explore/dataset/ods032/),
qui définissent explicitement `Datetime` comme début d'intervalle et P10/P90
comme bornes basse/haute. Cette seconde référence éclaire la convention ; elle
ne remplace pas la définition propre à ODS001.

## Mesures et qualité

Les huit colonnes `*_mw` sont copiées de la source, avec leurs `NULL` et valeurs
numériques d'origine : charge réelle, prévisions `most_recent`, `day_ahead`
(publication 18 h) et `week_ahead`, et bornes P10/P90 des deux premières
prévisions. La convention P10 basse et P90 haute est celle déjà utilisée par
le pipeline de qualité. L'examen des données confirme zéro paire P10 > P90 et
une prévision hors de ses bornes.

Quatre colonnes `*_quality_status` décrivent les familles charge, most recent,
day-ahead et week-ahead :

| Statut | Règle |
| --- | --- |
| `available` | Valeur présente sans anomalie couverte par les règles ci-dessous. |
| `missing` | Valeur `NULL` et fin de l'intervalle (`interval_start_utc` + 15 minutes) au plus tard à `quality_as_of_utc`. |
| `future` | Valeur `NULL` et fin de l'intervalle après `quality_as_of_utc` ; inclut l'intervalle en cours. |
| `suspect` | Valeur présente négative, ou prévision dont l'intervalle P10/P90 est invalide, ou anomalie explicitement identifiée. |

`quality_as_of_utc` est une référence UTC unique et stockée sur chaque ligne.
Par défaut, elle prend `run_started_at` de dbt. Pour reconstruire exactement la
classification avec une référence choisie, passer par exemple
`--vars "{quality_as_of_utc: '2026-10-19T16:55:23.562089+00:00'}"` à `dbt run`.
La classification ne suppose aucun délai de publication ; une valeur tardive
mais `NULL` après la fin de l'intervalle est `missing`.

Les colonnes `most_recent_interval_status` et `day_ahead_interval_status`
qualifient les triplets prévision/P10/P90. La première règle applicable gagne :

1. `incomplete` si une des trois valeurs manque ;
2. `negative_bound` si P10 ou P90 est négatif ;
3. `reversed_bounds` si P10 > P90 ;
4. `forecast_outside_bounds` si la prévision est hors de [P10, P90] ;
5. `valid` autrement.

Ces règles signalent les incohérences sans remplacer ni exclure une mesure.
L'examen du 19 octobre 2026 n'a trouvé aucun triplet incomplet ou inversé. Le seul triplet
`forecast_outside_bounds` concerne `most_recent_forecast_mw = 846` le
**2015-06-24 03:30 UTC** (05:30 en Belgique), avec P10 = 7 791,92 MW et
P90 = 8 699,21 MW. Cette ligne reste dans le fait, est `suspect` et porte
`most_recent_anomaly_code = known_2015_06_24_most_recent_846_mw`. Le code est
lié à cet instant et à cette valeur exacts ; aucun seuil général n'est inféré.

## Utilisation analytique

- Moyenne MW : `avg(total_load_mw)` sur des créneaux comparables et présents.
- Pic MW : `max(total_load_mw)` sur la période choisie.
- Énergie MWh : `sum(total_load_mw * 0.25)` uniquement si la mesure MW est bien
  la puissance moyenne de chaque intervalle complet de 15 minutes. Vérifier la
  sémantique de publication et la couverture avant cet usage.
- MAE : `avg(abs(forecast_mw - total_load_mw))` ; RMSE :
  `sqrt(avg(power(forecast_mw - total_load_mw, 2)))` ; biais :
  `avg(forecast_mw - total_load_mw)`. Calculer chaque indicateur sur les seules
  paires non `NULL`, avec le même filtre temporel et le même type de prévision.

La source ne conserve pas la date de publication ni la version de chaque
prévision. En particulier, `most_recent` ne permet pas de reconstituer un
backtest strictement « tel que connu à l'époque ». Les statuts ne sont pas une
validation statistique générale ; `available` ne signifie pas valeur certifiée.
Les clés et relations sont contrôlées par des tests dbt, sans contrainte physique
de clé primaire ou étrangère dans PostgreSQL.

Le schéma effectif contient 20 colonnes : deux `TIMESTAMPTZ`
(`interval_start_utc`, `quality_as_of_utc`), trois entiers (`date_key`,
`time_key`, `utc_offset_minutes`), huit `DOUBLE PRECISION` pour les mesures et
sept `TEXT` pour les statuts et le code d'anomalie. La matérialisation dbt en
table ne pose aucune contrainte physique `NOT NULL` ; les exigences de présence
sont vérifiées par les tests dbt.

## Exécution et vérification du 19 octobre 2026

Depuis la racine du dépôt avec `dbt/profiles.yml` local :

```powershell
.\.venv-dbt\Scripts\dbt.exe parse --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe run --select fact_load_15m --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe test --select fact_load_15m --project-dir .\dbt --profiles-dir .\dbt
```

Les trois commandes ont réussi avec dbt Core 1.11.16 et dbt-postgres 1.11.0
sur PostgreSQL 18.6. La table `analytics.fact_load_15m` contient **414 336**
lignes et 414 336 timestamps UTC distincts ; les **28 tests** du fait passent.
Les tests vérifient aussi l'égalité exacte des huit mesures, `NULL` compris,
avec `staging.elia_load`.

La référence de cette exécution est `2026-10-19 16:55:23.562089 UTC`. La charge
réelle compte 413 598 `available`, 137 `missing` et 601 `future`. La prévision
most recent compte 414 335 `available` et 1 `suspect`. La day-ahead compte
414 336 `available`. La week-ahead compte 414 144 `available` et 192 `missing`.

Le 2025-03-30 contient 92 lignes locales dans le fait ; le 2025-10-26 en
contient 100, dont quatre paires partageant les clés locales avec des offsets
60 et 120 minutes. La comparaison avant/après conserve les 18 objets `staging`
(nom, type, `relfilenode`), les 414 336 lignes et bornes temporelles source,
ainsi que les volumes et bornes de `dim_date` et `dim_time`. Les SQL de cette
étape n'écrivent que dans `analytics`.
