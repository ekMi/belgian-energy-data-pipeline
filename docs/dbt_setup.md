# dbt : installation et transformations

Le projet `dbt/` utilise dbt Core 1.11 et l'adaptateur PostgreSQL dans un
environnement Python 3.13 séparé. Il lit la source `staging.elia_load` et
matérialise trois tables dans `analytics` : `dim_date`, `dim_time` et
`fact_load_15m`. Les modèles sont reconstruits en tables, pas mis à jour de
façon incrémentale. Voir les contrats des [dimensions](temporal_dimensions.md)
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

# Écriture des trois tables analytics, puis tests, après le même précontrôle
.\.venv\Scripts\python.exe -B src/dbt_guard.py
```

Le lanceur charge `.env` avec `get_connection_params()` sans écraser les
variables du processus. Il transmet explicitement les cinq variables `PG*`
au `dbt.exe` de `.venv-dbt`, en imposant le projet, le profil et la cible
`local`. Avant `build`, il exige les variables complètes, `PGDATABASE` égal
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
enchaîne construction et tests. Ne lancer la construction qu'après avoir
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

L'acquisition des exports, l'orchestration et l'actualisation Power BI ne sont
pas automatisées. Les environnements `.venv-dbt/`, `dbt/target/`,
`dbt/logs/` et le profil local sont exclus de Git.
