# dbt : installation et transformations

Le projet `dbt/` utilise dbt Core 1.11 et l'adaptateur PostgreSQL dans un
environnement Python 3.13 séparé. Il lit la source `staging.elia_load` et
matérialise trois modèles (`dim_date`, `dim_time`, `fact_load_15m`) ainsi que
le seed `belgian_public_holidays` dans `analytics`. Les modèles sont reconstruits
en tables, pas mis à jour de façon incrémentale. Voir les contrats des [dimensions](temporal_dimensions.md)
et du [fait](fact_load_15m.md).

## Installation et profil local

Depuis la racine du dépôt, dans PowerShell :

```powershell
py -3.13 -m venv .venv-dbt
.\.venv-dbt\Scripts\python.exe -m pip install -r dbt\requirements.txt
.\.venv-dbt\Scripts\python.exe -m pip check
Copy-Item dbt\profiles.yml.example dbt\profiles.yml
```

`dbt/profiles.yml` est local et ignoré par Git. Il doit rester identique à
l'exemple versionné. Celui-ci lit `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`
et `PGPASSWORD` dans le processus et cible le schéma `analytics`.
Contrairement aux scripts Python, dbt ne charge pas le fichier `.env` :
mettre ces variables dans la session avant de l'appeler, sans afficher ou
versionner le mot de passe. Exemple pour une session PowerShell :

```powershell
$env:PGHOST = 'localhost'
$env:PGPORT = '5432'
$env:PGUSER = 'energy_app'
$securePassword = Read-Host 'Mot de passe PostgreSQL' -AsSecureString
$env:PGPASSWORD = [System.Net.NetworkCredential]::new('', $securePassword).Password
Remove-Variable securePassword
```

Le schéma cible du profil est déjà `analytics`. Ne pas ajouter
`+schema: analytics` aux modèles : la convention dbt pourrait alors viser
`analytics_analytics`. Le rôle dbt doit pouvoir lire `staging.elia_load`
et créer/remplacer ses tables dans `analytics`.

## Diagnostic, construction et tests

```powershell
# Lecture/configuration ; debug vérifie aussi la connexion PostgreSQL
.\.venv-dbt\Scripts\dbt.exe --version
.\.venv-dbt\Scripts\dbt.exe debug --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe parse --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe ls --project-dir .\dbt --profiles-dir .\dbt

# Précontrôle sans reconstruction (connexion et requête en lecture seule)
.\.venv\Scripts\python.exe -B src/dbt_guard.py --check-only

# Chargement du seed et reconstruction des modèles analytics avec leurs tests
.\.venv\Scripts\python.exe -B src/dbt_guard.py
```

Le lanceur charge `.env` avec `get_connection_params()` sans écraser les
variables du processus. Il transmet explicitement les cinq variables `PG*`
au Python de `.venv-dbt` via `src/dbt_entrypoint.py` en mode isolé (`-I`),
en imposant le projet, le profil et la cible `local`. Avant `build`, il exige
les variables complètes, `PGDATABASE` égal
à `belgian_energy`, un profil local identique à l'exemple attendu, et une
connexion PostgreSQL dont la base, l'utilisateur, l'adresse et le port
correspondent à la configuration. Un refus (code 1) invite à corriger les
variables ou à recopier l'exemple de profil ; aucun mot de passe n'est affiché.
Un appel manuel direct à `dbt.exe build` contourne ce précontrôle. En
production, restreindre aussi les droits du rôle PostgreSQL, protéger les
secrets et contrôler les changements de configuration et du code entre le
précontrôle et l'exécution.

`dbt run` matérialise les modèles sans exécuter tous les tests ;
`dbt test` exécute les tests sans reconstruire les tables. `dbt build`
charge les seeds et enchaîne construction et tests dans l’ordre des dépendances.
Ne lancer la construction qu'après avoir
vérifié la base et accepté les effets dans `analytics`. Elle ne modifie pas
`staging`. Le rapport Power BI connecté à ces tables est actualisé
**manuellement** après reconstruction réussie.

## État vérifié et limites

Le 26 octobre 2026, une reconstruction des trois modèles et 46 tests dbt
a réussi après ingestion. Ce résultat est daté ; il ne garantit pas le résultat
d'une nouvelle exécution. Les mesures source et leurs `NULL` sont conservés
dans le fait. La classification `missing`/`future` dépend de
`quality_as_of_utc` (par défaut le début du run dbt), sans délai de
publication Elia validé. Pour une reconstruction reproductible, voir
[`fact_load_15m.md`](fact_load_15m.md) et son exemple `--vars`.

L'acquisition et l'orchestration Prefect sont décrites dans [prefect.md](prefect.md).
L'actualisation Power BI reste manuelle. Les environnements `.venv-dbt/`, `dbt/target/`,
`dbt/logs/` et le profil local sont exclus de Git.

## Référentiel des jours fériés belges

```powershell
# Régénération déterministe ; aucune connexion SQL ni dépendance externe
.\.venv\Scripts\python.exe -B scripts/generate_belgian_holidays.py
.\.venv\Scripts\python.exe -B -m pytest tests/unit/test_belgian_holidays.py
# Construction complète protégée, sans ingestion
.\.venv\Scripts\python.exe -B src/dbt_guard.py
```

Le CSV UTF-8 est versionné : 260 jours fériés entre 2015 et 2040 inclus.
`dbt_project.yml` configure `seed-paths` et force `holiday_date` en PostgreSQL
`date`. `dim_date` dépend de `ref('belgian_public_holidays')` : le build complet
charge le seed et contrôle ses dates avant de construire la dimension. Aucun
package dbt ni ajout aux requirements n'est nécessaire. `dbt run` seul ne
charge pas le seed ; sur une base neuve, utiliser `dbt build`. Pour une
sélection ciblée, inclure les parents avec `--select +dim_date`.

Le lancement protégé et celui de Jenkins exécutent `build` sans filtre.
La validation CI exige maintenant la réussite du seed en plus des trois
modèles et des tests. Prefect passe par le pipeline métier et le même
`locked_build()` : aucune nouvelle tâche seed n'est nécessaire. Si le pipeline
juge dbt déjà à jour en l'absence de nouveaux batchs, un changement du calendrier
seul nécessite le lancement manuel protégé ci-dessus ; l'état des batchs ne
représente pas les changements de code ou de seed.

Les cinq colonnes calendaires et la limite des jours ouvrés (sans jours de
remplacement propres aux entreprises) sont décrites dans le
[contrat dimensionnel](temporal_dimensions.md#jours-fériés-et-jours-ouvrés).
Après le build, actualiser manuellement Power BI pour retrouver les catégories
`Férié`, `Week-end` et `Jour ouvré` du segment `day_type`.


### Validation du 6 novembre 2026

Le précontrôle `dbt_guard.py --check-only` et `pip check` de `.venv-dbt` ont
réussi. Un `dbt build --no-partial-parse` complet, via l'entrée isolée du projet
et sous `locked_build()`, a réussi dans un **schéma temporaire** avec profil
temporaire : 3 modèles, 1 seed et **56 tests**, sans avertissement ni échec.
La validation des manifest/run_results par `ci.dbt_build.validate_results()`
a également réussi. Le seed contient 260 dates distinctes et la dimension
4 335 dates distinctes, du 1er janvier 2015 au 13 novembre 2026. Le 15 août
2026 confirme la priorité du férié sur le samedi.

Le schéma temporaire a été supprimé. Les identités et fichiers physiques des
objets existants de `staging` et `analytics`, ainsi que le nombre de lignes et
les bornes de `staging.elia_load`, sont identiques avant/après. Aucun build
n'a reconstruit les tables analytics existantes. Cette validation locale ne
constitue pas une exécution sur l'agent Jenkins avec ses identifiants de test.
Les artefacts locaux de revue sont dans `reports/holiday-review/` (ignoré par Git).


Les lanceurs CI locaux ont validé **497 tests unitaires** et **92 tests Prefect**,
sans échec ni test ignoré. Ruff (`check` et `format --check`) a réussi sur tout
le dépôt. Le contrôle des fichiers modifiés/nouveaux n'a détecté aucune
signature de secret ; `.env`, le profil local et les rapports restent ignorés.
Le générateur et le CSV ne sont pas ignorés et restent à ajouter au prochain
commit manuel avec les autres changements utiles. Aucun commit ni push
n'a été effectué.
