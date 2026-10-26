# Phase 03C-1 — dbt local

Le sous-projet `dbt/` utilise dbt Core 1.11.16 et `dbt-postgres` 1.11.0 dans un
environnement Python 3.13 séparé. Il déclare uniquement la source existante
`staging.elia_load` (`source('elia', 'elia_load')`). Aucun modèle, test de source,
seed ou snapshot ne crée d'objet PostgreSQL à cette étape.

## Installation PowerShell

Depuis la racine du dépôt :

```powershell
py -3.13 -m venv .venv-dbt
.\.venv-dbt\Scripts\python.exe -m pip install -r dbt\requirements.txt
.\.venv-dbt\Scripts\python.exe -m pip check
Copy-Item dbt\profiles.yml.example dbt\profiles.yml
```

Le fichier `dbt/profiles.yml` reste local et ignoré par Git. Le modèle de profil
ne contient aucun secret : il lit `PGHOST`, `PGPORT`, `PGUSER` et `PGPASSWORD` de
l'environnement, comme le pipeline Python. `dbname` vaut `belgian_energy` et le
schéma cible vaut `analytics`. Exemple de configuration de la session :

```powershell
$env:PGHOST = 'localhost'
$env:PGPORT = '5432'
$env:PGUSER = 'energy_app' # adapter au compte autorisé localement
$securePassword = Read-Host 'Mot de passe PostgreSQL' -AsSecureString
$env:PGPASSWORD = [System.Net.NetworkCredential]::new('', $securePassword).Password
```

Le schéma `staging` figure dans la déclaration de source et reste géré par le
pipeline d'ingestion. Le profil cible directement `analytics`. Les futurs modèles
sans configuration `+schema` seront donc placés dans `analytics`. Ne pas leur
ajouter `+schema: analytics` : la convention par défaut de dbt produirait
`analytics_analytics`.

## Vérification sans matérialisation

```powershell
.\.venv-dbt\Scripts\dbt.exe --version
.\.venv-dbt\Scripts\dbt.exe debug --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe parse --project-dir .\dbt --profiles-dir .\dbt
.\.venv-dbt\Scripts\dbt.exe ls --resource-type source --project-dir .\dbt --profiles-dir .\dbt
```

`dbt debug` vérifie également la connexion, qui exige des identifiants réels et
les droits PostgreSQL adaptés. `dbt parse` et `dbt ls` vérifient le projet et la
source sans créer d'objet en base. Ne pas lancer `dbt run` ou `dbt build` pendant
03C-1.

## Résultat de la vérification du 19 octobre 2026

- Python 3.13.16 ; dbt Core 1.11.16 ; adaptateur Postgres 1.11.0 ; `pip check`
  sans dépendance cassée. Le client local `psql` indique PostgreSQL 18.6. La
  version du serveur n'a pas pu être lue sans connexion authentifiée.
- `dbt parse` : succès. `dbt ls --resource-type source` : une source,
  `source:belgian_energy_analytics.elia.elia_load`.
- `dbt debug` : profil et projet valides ; test de connexion échoué avec des
  identifiants fictifs de validation. Reprendre la commande ci-dessus avec les
  véritables variables locales pour confirmer la compatibilité avec le serveur
  PostgreSQL 18 et les permissions du compte.
- Aucune commande SQL de modification ni matérialisation dbt n'a été exécutée.
  Sans connexion authentifiée, l'état des objets PostgreSQL ne peut pas être
  comparé directement avant/après. Le client dbt n'a établi aucune connexion
  authentifiée durant ces vérifications.

`dbt/target/`, `dbt/logs/`, `dbt/dbt_packages/`, `.venv-dbt/` et le profil local
sont ignorés par Git. Les dépendances directes sont figées dans
`dbt/requirements.txt`; elles sont isolées du `requirements.txt` d'ingestion.
