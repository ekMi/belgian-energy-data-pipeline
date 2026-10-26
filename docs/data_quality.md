# Qualité des données ODS001

Ce guide décrit les règles **actuellement exécutées** par Python, leurs périmètres et les observations datées. Les statuts analytiques du fait dbt sont détaillés dans [fact_load_15m.md](fact_load_15m.md).

## Niveaux et périmètres

La validation structurelle refuse les en-têtes, clés, dates ou nombres invalides avant l'écriture des observations. Les règles de qualité produisent des résultats avec identifiant, version, dénominateur, nombre d'anomalies, exemples et sévérité. Un `ERROR` bloque le lot. Un `WARNING` n'empêche pas le commit ; il signale une anomalie ou une incertitude à examiner. Les résultats sont des **occurrences de règles**, ni un nombre de lignes distinctes ni un volume de MWh.

Chaque ingestion évalue deux périmètres dans la transaction :

| Périmètre | Ce qui est contrôlé |
| --- | --- |
| `incoming_file` | Toutes les observations du CSV, y compris `UNCHANGED`. La continuité temporelle du fichier est désactivée si `--input-scope partial`. |
| `consolidated_range` | Les valeurs stockées pour les clés reçues après insertion/correction, ainsi que les arêtes entre chaque clé reçue et ses voisins stockés immédiats. Ce n'est pas un balayage de toute la base. |

Les deux périmètres peuvent signaler la **même anomalie**. L'ingestion du 26 octobre 2026 a produit 1 854 occurrences `WARNING` : 927 dans chaque périmètre. Ce total ne signifie pas 1 854 observations distinctes. Le contrôle `technical.consolidated_matches_input` est bloquant si une clé reçue manque ou si ses valeurs stockées diffèrent du fichier accepté. Une erreur consolidée annule les inserts, corrections, historiques et rapports de succès. Un rapport d'échec `consolidated_range` peut porter `evaluation_state=rolled_back` : il décrit un état transactionnel abandonné, pas les valeurs actuellement stockées.

## Règles du pipeline

| Règle | Sévérité | Interprétation |
| --- | --- | --- |
| `technical.unique_key`, `technical.quarter_hour` | `ERROR` | Clé UTC/résolution unique ; instants alignés au quart d'heure et `PT15M`. |
| `technical.continuity`, `technical.consolidated_continuity` | `WARNING` | Écart entre instants consécutifs du fichier continu, ou entre voisins stockés touchant une clé entrante. Pas d'inférence au-delà des bornes. |
| `completeness.N.historical` | `WARNING` pour les deux champs nullable, `ERROR` pour les champs requis | Valeur `NULL` à ou avant `as_of - 24 h`. Les champs requis sont normalement interceptés au parsing. |
| `completeness.N.pending` | `WARNING` | Valeur `NULL` après cette coupure ; future ou en attente de publication, sans conclure à une perte historique. |
| `business.N.negative`, `business.N.outlier` | `WARNING` | Valeur négative ; écart à la médiane supérieur à six fois le MAD ajusté, avec au moins 20 valeurs et MAD non nul. |
| `business.3.interval`, `business.6.interval` | `WARNING` | Prévision hors de son intervalle P10/P90 ou bornes inversées, lorsque les trois valeurs existent. |
| `technical.ingestion`, `technical.consolidated_matches_input` | `ERROR` | Tentative échouée ou incohérence entre entrée acceptée et stockage consolidé. |

La **complétude technique** signifie que les clés, valeurs obligatoires et contraintes de format ont passé les contrôles. Elle ne prouve pas la **complétude métier** : couverture temporelle attendue, publication effective des mesures et valeurs interprétables. La continuité ne couvre que les instants observés dans les périmètres décrits ci-dessus. Le seuil de grâce de 24 heures pour les `NULL` est une règle actuelle du code, **pas un SLA Elia validé**. Les seuils d'anomalie statistique restent exploratoires et ne remplacent ni ne corrigent les valeurs.

Les timestamps d'entrée comportent un décalage explicite et sont normalisés en UTC. L'heure civile belge est obtenue avec `Europe/Brussels` dans les modèles dbt. À l'automne, deux instants UTC distincts peuvent partager le même quart d'heure local ; conserver le timestamp UTC et l'offset évite leur confusion. Voir [dimensions temporelles](temporal_dimensions.md).

## Rapports et historique

`staging.quality_run` conserve le batch, la date d'analyse, `as_of`, le périmètre et les paramètres ; `staging.quality_result` conserve les règles et leurs résultats. Les rapports sont ajoutés, jamais écrasés. `staging.elia_load_correction` conserve les huit anciennes et nouvelles mesures et les batchs de provenance pour chaque correction autorisée. Les valeurs inchangées gardent leur producteur précédent.

Après correction, `ingestion_batch_id` désigne le producteur de la **version courante** ; filtrer la table d'observations par un ancien batch ne reconstitue donc pas son export initial. Utiliser les rapports sauvegardés et le CSV original immuable identifié par SHA-256. Le mode `historical` du diagnostic lit les rapports existants ; le mode `current` réévalue seulement les versions courantes encore attribuées au batch. Voir [exploitation](operations.md) et [corrections](authorized_corrections.md).

## Constats historiques à ne pas confondre avec les règles

L'investigation de l'export étudié a relevé **7 charges réelles historiques manquantes**, **727 charges réelles en attente**, **192 prévisions week-ahead historiques manquantes** et une prévision historique incompatible avec son intervalle P10/P90 (la valeur 846 MW du 24 juin 2015). Quatre charges manquent à la première occurrence de l'heure locale répétée le 31 octobre 2021 ; la cause de cette observation n'est **pas établie**. Ces nombres décrivent un examen daté et ne sont ni des seuils ni des invariants du pipeline.

La fraîcheur des dernières mesures réelles a été examinée par une requête SQL avec un seuil **provisoire de 24 heures**. Ce contrôle de fraîcheur de publication n'est pas automatisé et ce seuil n'a pas été validé comme SLA Elia. Il est distinct de la coupure `as_of - 24 h` utilisée par la règle de complétude des valeurs `NULL`. Une surveillance périodique de fraîcheur, après validation d'un horizon de publication, reste une évolution possible.

Exemple de diagnostic ponctuel **en lecture seule**, à interpréter selon l'heure de publication réelle de la source :

```sql
SELECT max(timestamp_utc) AS derniere_charge_reelle_utc,
       now() - max(timestamp_utc) AS retard_apparent,
       now() - max(timestamp_utc) > interval '24 hours' AS depasse_seuil_provisoire
FROM staging.elia_load
WHERE total_load_mw IS NOT NULL AND timestamp_utc <= now();
```

## Schéma et vérification

Sur une nouvelle base, appliquer manuellement `sql/01_create_schema.sql` à `sql/04_add_incremental_tracking.sql` dans l'ordre, après revue et avec le compte propriétaire approprié. Sur une base existante, examiner les prérequis de chaque migration ; elles ne sont ni lancées par Python ni destinées à être rejouées. La migration qualité est `sql/03_add_quality_tracking.sql` ; le [contrat de schéma](incremental_contract.md) détaille la migration 04. Les tests PostgreSQL utilisent exclusivement la base jetable décrite dans [integration_tests.md](integration_tests.md). Les rapports et diagnostics courants sont détaillés dans [operations.md](operations.md).
