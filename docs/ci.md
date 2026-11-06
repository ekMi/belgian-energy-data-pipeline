# Intégration continue — Jenkins

Le `Jenkinsfile` Declarative s'exécute sur l'agent **windows-ci**, sous
`jenkins_agent` (workspace sous `D:\JenkinsAgent`). Le controller garde **0
exécuteur**. Le job suit `main` avec le SCM polling `H/5 * * * *`.
Le Python de démarrage est `C:\Program Files\Python313\python.exe` (3.13).
Aucun chemin de profil Windows personnel n'est nécessaire.

## Stages et dépendances

| Stage | Environnement et contrôle |
| --- | --- |
| Checkout | Nettoyage de `reports/`, puis checkout SCM |
| Setup Python | `.venv`, versions de `requirements.txt`, `pip check` |
| Ruff | Lint et contrôle du format sur tout le dépôt |
| Unit Tests | Tests unitaires, fichier Prefect affecté explicitement au stage suivant |
| Prefect Tests | `.venv-prefect`, `requirements-prefect.txt`, les deux modules Prefect |
| Integration Tests | `.venv`, tests PostgreSQL et migrations existants |
| dbt Parse | `.venv-dbt`, `dbt/requirements.txt`, profil sans secret, sans connexion SQL |
| dbt Build & Tests | Jeu synthétique, `dbt build`, vérification et nettoyage |

`ci/setup_env.ps1` crée les environnements absents. Il compare chaque version
figée aux distributions installées, puis ne lance `pip install` que si nécessaire.
`pip check` reste systématique. Les snapshots Windows UTF-16 et UTF-8 avec BOM
sont acceptés. Les trois environnements restent indépendants.

## PostgreSQL et credentials

Créer dans Jenkins un credential **Username with password**, ID
`belgian-energy-postgres-test`, utilisateur **energy_test_app**, mot de passe
du rôle de test. Le serveur visé est `127.0.0.1:5432`, base existante
**belgian_energy_test**, PostgreSQL **18**. Le rôle doit pouvoir créer et
supprimer `staging` et `analytics` ; voir [la préparation manuelle](integration_tests.md).
Les plugins Jenkins Pipeline, Git, Credentials Binding et JUnit sont nécessaires.

Les scripts exigent les cinq `TEST_PG*` et refusent toute autre base ou tout
autre utilisateur. Chaque connexion des fixtures, ainsi que la préparation et
le nettoyage dbt, contrôlent `current_database()`, `current_user`, `session_user`
et `server_version_num` avant écriture. Les réglages libpq hérités sont retirés.
Le `.env` métier est désactivé dans pytest et n'est jamais chargé par le script dbt.
Ne jamais utiliser les credentials de `energy_app` ni la base `belgian_energy`.

Les stages PostgreSQL sont séquentiels. `disableConcurrentBuilds()` met les builds
du même job en attente ([Jenkins](https://www.jenkins.io/doc/book/pipeline/syntax/#options)).
Les fixtures et le script dbt partagent aussi le verrou consultatif de session
`(81002, 1)` : un autre run utilisant ces outils échoue avant toute mutation
s'il ne peut l'acquérir, même depuis un autre job/workspace. Aucun plugin de
verrouillage supplémentaire n'est nécessaire. Les outils externes doivent
respecter ce verrou ; réserver cette base aux tests.

Le script `ci/dbt_build.py` réutilise `sql/01_create_schema.sql` sans modification
et charge **197 observations synthétiques** : journées DST 2025 complètes (92 et
100 intervalles), bornes ISO, anomalie connue et mesures optionnelles absentes.
La référence qualité est figée au 1er novembre 2025. Le profil existant est copié
dans un dossier temporaire et adapté à un `DBT_ENV_SECRET_*` masqué par dbt.
Le script exige les trois tables analytiques non vides et un succès pour tous
les modèles/tests du manifest, sans sélection partielle, warning ni skip.
`src/dbt_guard.py` reste inchangé.

**Les schémas staging et analytics de la base de test sont jetables** : remise à
zéro avant le run, nettoyage dans `finally`, même après échec de dbt. Le verrou
reste détenu jusqu'à la fin du nettoyage. Après arrêt brutal de l'agent/PC, le
nettoyage n'est pas garanti ; le run suivant remet les schémas à zéro sous verrou.

## Isolation Prefect et rapports

`ci/run_tests.py prefect` exécute automatiquement
`tests/unit/test_prefect_orchestration.py` et `tests/prefect/test_flow_engine.py`.
Un dossier temporaire sur le disque du workspace contient le home, les profils,
le stockage et les fichiers temporaires Prefect. Le processus démarre hors du
dépôt, sans ses fichiers de paramètres ni les variables API/profil héritées.
Le harness démarre son propre serveur SQLite sur un port libre, puis l'arrête.
Aucun worker, deployment ou ingestion réelle n'est lancé. Les opérations de
fichiers des tests utilisent leurs répertoires temporaires, jamais l'inbox réelle.
Après `setup_env.ps1`, le stage Prefect appelle `ci/ensure_sqlalchemy_pure.ps1`.
Il exige SQLAlchemy **2.1.6** importable avec `has_compiled_ext() == False`.
Si nécessaire, il reconstruit cette seule dépendance en Python pur avec
`DISABLE_SQLALCHEMY_CEXT=1`, `--no-binary SQLAlchemy`, `--no-cache-dir`,
`--force-reinstall` et `--no-deps`, puis revérifie l'import et `pip check`.
Une installation conforme est conservée sans reconstruction. La variable de
compilation est restaurée après installation ; la politique Windows reste inchangée.
Voir aussi le [dépannage SQLAlchemy Windows](prefect.md#dépannage-sqlalchemy-sous-windows).

Les lanceurs pytest refusent les rapports vides, skips, erreurs et échecs ; ils
exigent aussi la présence des deux modules dans le rapport Prefect. Pytest reste
en erreur si aucun test n'est collecté. Les rapports distincts sont
`reports/unit.xml`, `prefect.xml`, `integration.xml` et `dbt.xml`.
Jenkins publie les rapports disponibles et archive `reports/**` dans `post always`,
y compris après échec. `reports/dbt-build/` conserve manifest, run_results et SQL
compilé ; `reports/dbt-logs/` conserve les logs dbt. Les fichiers de la phase parse
sont distincts. Aucun résultat d'un ancien build n'est réutilisé.

## Validation locale et recette Jenkins

Depuis la racine du dépôt, dans PowerShell :

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File ci/setup_env.ps1 -Environment .venv -Requirements requirements.txt
powershell -NoProfile -ExecutionPolicy Bypass -File ci/setup_env.ps1 -Environment .venv-prefect -Requirements requirements-prefect.txt
powershell -NoProfile -ExecutionPolicy Bypass -File ci/ensure_sqlalchemy_pure.ps1 -Environment .venv-prefect
powershell -NoProfile -ExecutionPolicy Bypass -File ci/setup_env.ps1 -Environment .venv-dbt -Requirements dbt/requirements.txt
.\.venv\Scripts\ruff.exe check --no-cache .
.\.venv\Scripts\ruff.exe format --check --no-cache .
.\.venv\Scripts\python.exe -B ci/run_tests.py unit
.\.venv-prefect\Scripts\python.exe -B ci/run_tests.py prefect
# Après configuration explicite des cinq TEST_PG* (guide PostgreSQL) :
.\.venv\Scripts\python.exe -B ci/run_tests.py integration
.\.venv\Scripts\python.exe -B -m ci.dbt_build
```

Pour la recette finale, versionner les changements sur `main`, puis lancer
**Build Now** ou attendre le polling. Vérifier les huit stages verts, zéro test
ignoré, les quatre rapports JUnit et les artefacts dbt. Le log doit confirmer
les trois modèles, tous les tests dbt et le nettoyage. Déclencher deux builds
pour vérifier la mise en attente du second. Tester aussi un commit sans lancement
manuel pour confirmer le polling. Ne jamais lancer le pipeline métier pour cette
recette. Une validation locale sans credentials ne prouve pas la réussite des
stages PostgreSQL, dbt connecté ou de l'exécution sous `jenkins_agent`.


### Seed des jours fériés

Le build complet charge `belgian_public_holidays` avant `dim_date`, grâce au
`ref()` du modèle. La validation des artefacts exige les trois modèles, ce seed
et tous les tests activés ; les statuts seed autres que `success` sont refusés.
Les tests unitaires couvrent la génération déterministe des 260 lignes
2015-2040 et les dates mobiles. Le contrôle dbt couvre les dates uniques/non
NULL, dix fêtes par année, la plage couverte et les catégories de la dimension.
Le CSV doit accompagner le code dans le checkout Jenkins ; il n'est pas généré
pendant l'ingestion ou par une nouvelle tâche Prefect.
