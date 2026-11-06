# Dimensions temporelles

Les modèles dbt `dim_date` et `dim_time` sont des tables dans le schéma cible
`analytics`. Ils lisent `staging.elia_load` sans le modifier. Le modèle
`fact_load_15m` utilise leurs clés locales.

## Clés et calendrier

`dim_date` contient une ligne par date civile belge entre les bornes inclusives
de `timestamp_utc AT TIME ZONE 'Europe/Brussels'` dans la source. Les dates sont
générées sans trou par PostgreSQL. `date_key` est l'entier `YYYYMMDD` ;
`date_local` est un `DATE`. Une nouvelle ingestion étend la dimension à la
prochaine exécution de `dbt run`.

`year`, `quarter`, `month` et `day_of_month` suivent le calendrier civil.
`iso_year`, `iso_week` et `day_of_week` suivent ISO 8601 : lundi = 1, dimanche =
7 ; l'année ISO peut différer de l'année civile. `is_weekend` désigne samedi et
dimanche. Les saisons météorologiques sont `winter` (décembre-février),
`spring` (mars-mai), `summer` (juin-août) et `autumn` (septembre-novembre).
`month_name` et `season` sont en anglais.

`dim_time` est un référentiel fixe de 96 heures locales de quart d'heure,
`00:00` à `23:45`. `time_key` est l'entier `HHMM` (par exemple `15` pour
`00:15`) ; `time_local` est un `TIME`, sans date ni fuseau. `quarter_hour` va de
1 à 4 et `time_label` est l'heure `HH` sur deux chiffres.

## Jours fériés et jours ouvrés

Le seed versionné `dbt/seeds/belgian_public_holidays.csv` contient **260 lignes**,
soit dix jours fériés nationaux par an, de **2015 à 2040 inclus**. Il est joint
sur `date_local = holiday_date`, avec une seule ligne par date.

| Colonne | Définition |
| --- | --- |
| `is_weekend` | Booléen non NULL : samedi ou dimanche, même un jour férié. |
| `is_public_holiday` | Booléen non NULL : date présente dans le seed. |
| `holiday_name` | Nom français du jour férié ; NULL sinon. |
| `is_business_day` | Booléen non NULL : lundi à vendredi, hors jours fériés nationaux. |
| `day_type` | `Férié` si férié, sinon `Week-end` si samedi/dimanche, sinon `Jour ouvré`. |

La priorité `Férié` > `Week-end` > `Jour ouvré` fournit trois catégories
exclusives pour le segment interactif Power BI. Par exemple, le samedi
15 août 2026 est `Férié`, avec les deux indicateurs weekend/férié vrais et
`is_business_day = false`.

Les sept dates fixes sont le 1er janvier, le 1er mai, le 21 juillet, le
15 août, le 1er novembre, le 11 novembre et le 25 décembre. Les trois dates
mobiles sont le lundi de Pâques (Pâques + 1 jour), l'Ascension (+ 39) et le
lundi de Pentecôte (+ 50). Le générateur calcule Pâques selon l'algorithme
grégorien Meeus/Jones/Butcher, sans dépendance externe.

Ce calendrier suit les [dix jours fériés nationaux du SPF Emploi](https://emploi.belgique.be/fr/themes/jours-feries-et-conges/jours-feries).
La définition analytique de « jour ouvré » suppose une semaine du lundi au
vendredi. Elle exclut les jours de remplacement fixés par secteur ou entreprise,
les congés extralégaux, les fêtes régionales et les jours propres aux
administrations. Aucun report automatique au lundi n'est appliqué. Les règles
futures restent à réviser si la législation change.

Les tests dbt vérifient l'unicité et la non-nullité des dates du seed et de
la dimension, dix jours par année, les catégories et la cohérence des booléens
avec le seed. Une date de dimension hors de 2015-2040 fait échouer le contrôle
de couverture ; étendre le générateur, le seed et ce contrôle avant de charger
une telle période. Les tests Python figent les références 2015/2026 et un cas
de Pâques tardif (2038), le tri, les dates mobiles et la reproduction du CSV.

## Changement d'heure

Le modèle fact_load_15m convertit les timestamps UTC en heure belge.
Une journée normale a 96 observations à résolution PT15M, le passage
à l'heure d'été en a 92 et le retour à l'heure d'hiver en a 100. `dim_time`
reste toujours à 96 lignes. Lors du retour, les quatre créneaux `02:00` à
`02:45` apparaissent deux fois : deux faits pourront donc avoir les mêmes
`date_key` et `time_key`. Le fait conserve `interval_start_utc` et le
décalage UTC pour distinguer ces observations.

Les tests de dimensions contrôlent la grille de 96 créneaux, le calendrier
continu, les clés et les attributs ISO. Un test de référence construit les
instants UTC des deux transitions de 2025 et vérifie 92/100 observations,
92/96 heures locales distinctes et les répétitions de 02:00 lors du retour.
Ce test vérifie la conversion temporelle et la correspondance avec `dim_time` ;
il ne garantit pas l'exhaustivité de chaque journée dans la source ou le
fait. Les contrôles d'unicité du timestamp UTC et de conservation du décalage
horaire sont vérifiés par les tests du fait.

## Exécution et résultats du 19 octobre 2026

Depuis la racine du dépôt, avec le profil local `dbt/profiles.yml` configuré :

```powershell
.\.venv-dbt\Scripts\dbt.exe debug --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe parse --project-dir .\dbt --profiles-dir .\dbt
# Inclure le seed parent lors d'une reconstruction ciblée
.\.venv-dbt\Scripts\dbt.exe build --select +dim_date dim_time --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe test --select dim_date dim_time --project-dir .\dbt --profiles-dir .\dbt
```

`dbt debug` et `dbt parse` ont réussi avec dbt Core 1.11.16, dbt-postgres
1.11.0 et PostgreSQL serveur 18.6. `dbt run` a créé les deux tables dans
`analytics` : 4 316 dates de 2015-01-01 à 2026-10-25 et 96 créneaux. Aucun
schéma `analytics_analytics` n'a été créé. `dbt test` : 18 tests réussis,
aucun avertissement ni échec.

La source contient 92 observations et 92 heures locales distinctes le
2025-03-30 ; elle contient 100 observations et 96 heures locales distinctes le
2025-10-26, avec quatre créneaux locaux répétés. La comparaison avant/après
montre les mêmes 18 objets `staging` (nom, type et `relfilenode`) et les mêmes
414 336 lignes source, avec les mêmes bornes temporelles. Les SQL dbt de cette
étape n'écrivent que dans `analytics`.
