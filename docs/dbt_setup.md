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

`dbt/profiles.yml` est local et ignoré par Git. Le profil exemple lit
`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` dans le processus et cible
`belgian_energy` / `analytics`. Adapter `dbname` si nécessaire.
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

# Écriture des trois tables analytics, puis tests
.\.venv-dbt\Scripts\dbt.exe build --project-dir .\dbt --profiles-dir .\dbt
```

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
