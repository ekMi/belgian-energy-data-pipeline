# Orchestration locale avec Prefect 3

Ce guide part du pipeline existant sous Windows 11, Python 3.13, PostgreSQL 18
et dbt Core 1.11. Les migrations 01 à 05, `.env`, `.venv` et `.venv-dbt` doivent
déjà être configurés selon les guides [d'exploitation](operations.md) et
[dbt](dbt_setup.md). Prefect n'applique aucune migration.

## Les composants

- Un **flow** décrit une exécution suivie par Prefect. `energy-inbox` recherche
  un CSV ; il appelle `belgian-energy-pipeline` comme **sous-flow** et attend sa fin.
- Une **task** est une opération suivie dans un flow : `run-local-pipeline`
  lance le script métier dans un sous-processus et récupère son résultat.
- Un **deployment** enregistre le flow, ses paramètres, son pool et ses horaires.
- Le **work pool** `energy-local`, de type `process`, reçoit les runs à lancer.
- Un **worker** interroge ce pool et démarre les processus Python locaux.

| Deployment | Flow | Déclenchement |
| --- | --- | --- |
| `elia-manual` | `belgian-energy-pipeline` | Manuel, paramètre `csv_path`, sans horaire |
| `elia-inbox` | `energy-inbox` | Manuel ou Cron toutes les 5 minutes, `Europe/Brussels`, initialement **inactif** |
| `elia-api-daily` | `belgian-energy-api` | Manuel ou tous les jours à **10h00 Europe/Brussels**, initialement **inactif** |

Les deux deployments CSV utilisent la politique métier `elia-full` : les mises à
jour ordinaires de la source Elia, y compris `CHANGED`, sont acceptées et
historisées automatiquement, après les contrôles techniques habituels. La
[politique commune](authorized_corrections.md) est exécutée par le script métier,
pas réimplémentée dans Prefect. Le défaut des CLI et des fonctions Python
génériques reste `restricted` ; le deployment manuel déclare `elia-full` dans
ses paramètres et le parent inbox le transmet explicitement au sous-flow.

Un seul pool et un seul worker suffisent. Le parent traite **un seul CSV par
exécution**, dans l'ordre des noms (sans distinction de casse, puis nom exact).
Les autres fichiers attendent le passage suivant.

## Installer Prefect

Dans PowerShell, à la racine du dépôt :

```powershell
Set-Location 'D:\Utilisateurs\Mike\Projets\belgian-energy-data-pipeline'
# Seulement si cet environnement n'existe pas :
py -3.13 -m venv .venv-prefect
.\.venv-prefect\Scripts\python.exe -m pip install -r requirements-prefect.txt
.\.venv-prefect\Scripts\python.exe -m pip check
.\.venv-prefect\Scripts\prefect.exe version
```

`requirements-prefect.txt` fige Prefect **3.9.1** et les dépendances de
l'environnement de référence, avec pytest pour les tests d'orchestration.
Valider toute mise à jour avant de renouveler ce snapshot. Vérifier que
`prefect version` affiche 3.9.1 et Python 3.13. L'installation habituelle utilise
les wheels disponibles ; le dépannage SQLAlchemy ci-dessous est facultatif.

Les trois environnements restent séparés : Prefect s'importe dans
`.venv-prefect`, `local_pipeline.py` et `api_pipeline.py` utilisent `.venv\Scripts\python.exe`, et le
pipeline métier lance dbt avec `.venv-dbt`. L'activation des environnements
n'est pas nécessaire avec les exécutables explicites.

## Serveur, pool et worker

Ne pas lancer un second serveur ou un second worker si ceux-ci fonctionnent déjà.
Dans chaque nouveau terminal d'administration ou de worker, définir :

```powershell
Set-Location 'D:\Utilisateurs\Mike\Projets\belgian-energy-data-pipeline'
$env:PREFECT_API_URL = 'http://127.0.0.1:4200/api'
$env:ENERGY_PROJECT_ROOT = (Get-Location).Path
```

`ENERGY_PROJECT_ROOT` permet à l'étape `pull` du YAML de fixer le répertoire de
travail sans versionner un chemin personnel. Le définir lors de l'enregistrement
**et** du démarrage du worker. Réenregistrer les deployments si le dépôt change
de chemin. Le code résout les CSV relatifs depuis la racine du dépôt. Conserver
les cinq dossiers de données sur le **même disque local Windows** : le verrou et
le renommage utilisent les garanties Windows et ne visent pas les partages réseau.

Dans le terminal **serveur**, laisser actif :

```powershell
.\.venv-prefect\Scripts\prefect.exe server start --host 127.0.0.1 --port 4200
```

L'interface est sur <http://127.0.0.1:4200>. Les métadonnées du serveur sont
distinctes de PostgreSQL métier, habituellement sous `$HOME\.prefect`. Ne pas
les versionner ; `.prefect/` est aussi ignoré si ce stockage est placé au dépôt.

Dans le terminal d'**administration**, configurer le profil pour les futures
sessions, inspecter le pool puis le créer seulement s'il est absent :

```powershell
.\.venv-prefect\Scripts\prefect.exe config set PREFECT_API_URL=http://127.0.0.1:4200/api
.\.venv-prefect\Scripts\prefect.exe work-pool inspect energy-local
# Seulement si le pool n'existe pas :
.\.venv-prefect\Scripts\prefect.exe work-pool create --type process energy-local
```

Dans le terminal **worker**, après les variables communes, laisser actif :

```powershell
.\.venv-prefect\Scripts\prefect.exe worker start --pool energy-local --type process --name energy-local-worker --limit 1
```

`--limit 1` limite les runs de deployments pris par ce worker, y compris le
manuel. Le sous-flow est appelé directement : il n'utilise pas un autre créneau
de deployment. Le deployment `elia-inbox` possède aussi une limite de 1 avec
`CANCEL_NEW` : un déclenchement qui chevauche un run existant est annulé.

Un verrou Windows sur `data/processing/.inbox.lock` protège également deux
appels locaux directs du parent ; le second termine normalement avec `BUSY`.
Ce fichier reste présent, mais Windows libère le verrou à la fermeture du
processus. Ne pas supprimer ce fichier pendant une exécution.

Serveur **et** worker doivent rester actifs. Un serveur seul peut planifier
des runs mais n'exécute pas l'ingestion. Un PC éteint ou en veille ne traite
aucune acquisition ni ingestion. Au redémarrage, inspecter les runs en retard et annuler ceux qui ne
sont plus souhaités avant de relancer le worker.

## Enregistrer les deployments et les lancer dans l'interface

Depuis le terminal d'administration configuré :

```powershell
.\.venv-prefect\Scripts\prefect.exe deploy --all --prefect-file prefect.yaml --no-prompt
.\.venv-prefect\Scripts\prefect.exe deployment ls
.\.venv-prefect\Scripts\prefect.exe deployment inspect 'belgian-energy-pipeline/elia-manual'
.\.venv-prefect\Scripts\prefect.exe deployment inspect 'energy-inbox/elia-inbox'
.\.venv-prefect\Scripts\prefect.exe deployment schedule ls 'energy-inbox/elia-inbox'
```

L'enregistrement ne déclenche pas le métier. Vérifier `active: false` pour le
Cron et l'absence de schedule sur `elia-manual`. La syntaxe est validée avec
les parseurs et modèles de Prefect **3.9.1 installé**, sans appel serveur.
La [documentation des schedules](https://docs.prefect.io/v3/concepts/schedules)
décrit `cron`, `timezone` et `active`.

Après changement de politique, réenregistrer les deployments pour mettre à jour
les paramètres de `elia-manual` ; aucun enregistrement ni lancement réel n'est
effectué par les tests. Le YAML remet le Cron à l'état inactif lors de son
enregistrement : ne le réactiver que volontairement.

Dans **Deployments**, choisir un deployment puis **Run** ou **Quick run** :

1. `elia-inbox` : une inbox vide vérifie `Completed` / `EMPTY` sans ingestion.
   Avec un CSV prêt, ce bouton lance réellement le pipeline.
2. `elia-manual` : renseigner `csv_path`, par exemple `data/raw/mon_export.csv`
   ou un chemin absolu. Le défaut reste `data/raw/ods001.csv`. Lancer seulement
   après validation de l'ingestion. Ce flow fonctionne indépendamment d'inbox
   et **ne déplace pas le CSV**. Les paramètres du deployment sont
   `source_policy=elia-full`, `input_scope=continuous`. Pour un import partiel
   exceptionnel, sélectionner `source_policy=restricted` et `input_scope=partial`.

Pour le parcours API, voir la procédure `elia-api-daily` ci-dessous.

Ne pas fournir au flow manuel un fichier déjà pris en charge par le parent.
Le verrou d'inbox ne couvre pas les appels directs au script métier ; les
protections PostgreSQL existantes restent en place.

## Déposer un CSV complet

L'opérateur doit déposer le **dernier export complet ODS001 provenant d'Elia**.
Le dossier n'authentifie pas les fichiers et ne prouve pas leur fraîcheur.
Ne pas y déposer d'extraits partiels ou de CSV d'une source non fiable.
La date de modification contrôlée ci-dessous concerne seulement la fin de copie,
pas la date d'extraction Elia. Le pipeline n'invente jamais `extracted_at` ; sans
date réelle fournie, elle reste inconnue. Un ancien export jamais ingéré peut
donc remplacer des valeurs plus récentes s'il est déclaré fiable à tort.

Copier sous une extension temporaire dans inbox, fermer la copie, puis renommer
en `.csv`. Exemple avec un nom final unique :

```powershell
New-Item -ItemType Directory -Force data/inbox | Out-Null
$depositName = 'elia_' + [guid]::NewGuid().ToString('N')
$temporary = Join-Path 'data/inbox' ($depositName + '.uploading')
Copy-Item -LiteralPath 'C:\Exports\elia_export.csv' -Destination $temporary -ErrorAction Stop
(Get-Item -LiteralPath $temporary).LastWriteTimeUtc = [DateTime]::UtcNow
Rename-Item -LiteralPath $temporary -NewName ($depositName + '.csv') -ErrorAction Stop
```

Le CSV doit être non vide, avoir une dernière modification datant d'au moins
**60 secondes**, puis garder la même taille et la même date de modification
pendant **1 seconde**. Les noms commençant par `.` ou `~`, les extensions non CSV,
et les segments `.tmp.`, `.temp.`, `.part.`, `.partial.`, `.crdownload.` sont ignorés.
Un fichier dont Windows refuse le déplacement reste dans inbox pour un passage
ultérieur. Ces contrôles ne prouvent pas qu'une copie suspendue est terminée :
le renommage final après fermeture de la copie reste la méthode recommandée.

## Cycle de vie et diagnostics

Le parent déplace le CSV dans `data/processing/`, attend la fin du sous-flow,
puis le classe et écrit `nom.csv.result.json` à côté. Le diagnostic UTF-8 conserve
statut, identifiants, compteurs, code de sortie et stdout/stderr disponibles.
Les logs du sous-processus remontent à Prefect **après sa fin**, pas en direct.

| Résultat métier | Dossier final | État Prefect normal |
| --- | --- | --- |
| `SUCCESS` | `data/archive/` | `Completed` |
| `NO_CHANGE` | `data/archive/` | `Completed` |
| `DBT_FAILED` | `data/failed/` | `Failed` pour la task, le sous-flow et le parent |
| Erreur technique, validation ou qualité | `data/failed/` | `Failed` |

`elia-full` réutilise le mécanisme de correction historisée avec le hash calculé
sous verrou et le motif `Automatic update from authoritative Elia ODS001 full export`.
Ce motif identifie une décision automatique, jamais une revue humaine. Les
contrôles de fraîcheur fondés sur une date d'extraction réellement connue restent
actifs. `DBT_FAILED` conserve l'ingestion déjà validée,
avec son batch et ses corrections historisées. Une sortie absente,
mal formée ou incompatible avec son code devient une erreur technique. Une
erreur de lancement avant création du sous-processus n'a pas de code de sortie.

Les corrections d'un export complet fiable ne produisent plus
`AWAITING_APPROVAL`. Si ce résultat inattendu arrive dans inbox, le parent échoue
et classe le CSV dans `failed/` pour diagnostic. La task sait encore lire ce
statut pour un lancement manuel **restrictif** : code CLI 1 et état Prefect
`Completed`, sans correction appliquée. Le flow manuel ne classe aucun fichier.

Prefect décrit l'exécution ; le statut métier décrit le résultat fonctionnel.
Lire les deux. `EMPTY` et `BUSY` concernent le parent sans appel métier. Les
résultats Python ne sont pas persistés dans le stockage de résultats Prefect ;
consulter les logs et les diagnostics locaux.

Les noms sont conservés ; un conflit ajoute `__<UUID>` au nom du CSV, sans
remplacement. Un diagnostic existant est également conservé. Aucun CSV n'est
supprimé automatiquement. Si le classement échoue, le CSV reste généralement
dans processing ; si seule l'écriture du diagnostic échoue, il se trouve déjà
dans son dossier final et le flow échoue.

Flows et task ont `retries=0` : aucune reprise automatique de l'ingestion ou de
dbt. Les passages suivants cherchent uniquement dans inbox ; ils ne reprennent
pas les fichiers de `failed/`, `pending_approval/` ou `processing/`.
`pending_approval/` n'est plus alimenté par inbox ; ses fichiers antérieurs
restent conservés et ne sont ni déplacés ni effacés par cette évolution.

## Activer, désactiver et vérifier le Cron

Laisser le Cron inactif pendant la validation. Pour l'activer volontairement,
relever l'ID de sa schedule puis exécuter :

```powershell
.\.venv-prefect\Scripts\prefect.exe deployment schedule ls 'energy-inbox/elia-inbox'
$scheduleId = 'REMPLACER_PAR_UUID_DE_LA_SCHEDULE'
.\.venv-prefect\Scripts\prefect.exe deployment schedule resume 'energy-inbox/elia-inbox' $scheduleId
.\.venv-prefect\Scripts\prefect.exe deployment schedule ls 'energy-inbox/elia-inbox'
# Désactivation :
.\.venv-prefect\Scripts\prefect.exe deployment schedule pause 'energy-inbox/elia-inbox' $scheduleId
.\.venv-prefect\Scripts\prefect.exe deployment schedule ls 'energy-inbox/elia-inbox'
```

Le bouton de la schedule dans l'interface donne le même contrôle. Réenregistrer
le YAML remet la schedule à `active: false`. Désactiver l'horaire n'arrête pas
une ingestion en cours ; inspecter aussi les runs déjà créés.

Dans **Flow Runs**, ouvrir le parent, son sous-flow puis sa task pour consulter
logs, paramètres et états. Les logs présentent `status`, `ingestion_batch_id`,
`build_id`, `rows_new`, `rows_changed` et le batch cible lorsque disponible.
Les compteurs peuvent valoir `null` selon le résultat métier. En CLI :

```powershell
.\.venv-prefect\Scripts\prefect.exe flow-run ls
$flowRunId = 'REMPLACER_PAR_UUID_DU_RUN'
.\.venv-prefect\Scripts\prefect.exe flow-run inspect $flowRunId
.\.venv-prefect\Scripts\prefect.exe flow-run logs $flowRunId
Get-Content -LiteralPath 'data/failed/mon_export.csv.result.json' -Encoding UTF8
```

## Revue manuelle et reprise après échec ou interruption

Les nouveaux exports complets de confiance ne nécessitent pas cette revue.
Pour un **ancien** CSV dans `pending_approval/`, déterminer d'abord sa provenance
et s'il représente encore le dernier export complet Elia. Ne pas remettre un
ancien export en inbox au seul motif qu'il attendait une approbation.

Pour un import partiel ou non fiable conservant la politique restrictive,
suivre le [guide des corrections](authorized_corrections.md).
Prévisualiser en lecture seule, examiner le rapport complet et vérifier le hash :

```powershell
$csv = 'data/pending_approval/mon_export.csv'
.\.venv\Scripts\python.exe -B src/orchestration_preview.py $csv
.\.venv\Scripts\python.exe -B src/merge_ods001.py $csv --full-report --output data/processed/revue_prefect.json
(Get-FileHash -LiteralPath $csv -Algorithm SHA256).Hash.ToLowerInvariant()
```

Après revue et autorisation explicite du fichier exact, lancer volontairement :

```powershell
.\.venv\Scripts\python.exe -B src/local_pipeline.py $csv --authorize-sha256 'HASH_VERIFIE_64_CARACTERES_MINUSCULES' --correction-reason 'Motif précis validé après revue'
$pipelineExitCode = $LASTEXITCODE
```

La CLI manuelle conserve le CSV en place. Vérifier **le JSON et le code**
(`SUCCESS` ou `NO_CHANGE`, code 0) avant de l'archiver manuellement. Un code 1
n'autorise aucune correction supplémentaire automatique. Les options de date
d'extraction et de référence restent celles du guide métier.

Après une erreur ou une interruption :

1. Désactiver la schedule, attendre la fin des runs actifs et vérifier qu'aucun
   sous-processus métier ou dbt ne travaille encore sur ce fichier. Une annulation
   Prefect ne prouve pas à elle seule l'arrêt d'un enfant.
2. Lire les logs, diagnostics et états des batchs/builds. Une interruption brutale
   peut laisser un build `RUNNING` et un CSV dans processing ; ne pas déduire de
   l'état Prefect qu'aucune écriture n'a eu lieu.
3. Corriger la cause puis remettre volontairement **un seul export complet Elia
   approprié** dans inbox, avec un nom unique. Ne pas recopier le même contenu
   en parallèle. Garder les imports partiels dans le parcours manuel restrictif.
4. Déclencher `elia-inbox`. Le pipeline revérifie contenu et base. Après
   `DBT_FAILED`, le même fichier peut retenter dbt sans annuler ni réappliquer
   l'ingestion validée. Les protections métier restent actives.

Exemple, après vérification de l'arrêt des processus :

```powershell
$csv = 'data/processing/mon_export.csv' # ou data/failed/mon_export.csv
$recoveryName = 'reprise_' + [guid]::NewGuid().ToString('N') + '.csv'
Rename-Item -LiteralPath $csv -NewName $recoveryName -ErrorAction Stop
$renamed = Join-Path (Split-Path $csv) $recoveryName
Move-Item -LiteralPath $renamed -Destination (Join-Path 'data/inbox' $recoveryName) -ErrorAction Stop
```

Conserver les anciens diagnostics. Pour archiver après succès d'une CLI manuelle,
utiliser de même un nom UUID unique et déplacer vers `data/archive/`, sans `-Force`.
Le verrou `.inbox.lock` est libéré par Windows même après un arrêt brutal ; sa
présence ne signifie pas qu'un traitement est actif. Le CSV dans processing
reste récupérable et n'est jamais repris tout seul.

## Arrêter et redémarrer

Désactiver la schedule, vérifier les runs encore planifiés et laisser finir les
traitements. Faire `Ctrl+C` dans le terminal worker, attendre sa sortie, puis
`Ctrl+C` dans le terminal serveur. Éviter de fermer brutalement une fenêtre
pendant une ingestion ou un build.

Pour reprendre, redémarrer le serveur, inspecter les runs en retard puis relancer
**le seul worker**, avec les mêmes variables et commandes. Les deployments sont
conservés dans les métadonnées du serveur ; ne pas recréer le pool. Réenregistrer
les deployments si le code ou la configuration a changé. Examiner processing
avant de réactiver volontairement le Cron.

## Dépannage SQLAlchemy sous Windows

L'environnement de référence a rencontré une erreur d'import de l'extension
compilée `_immutabledict_cy`. Reconstruire **SQLAlchemy 2.1.6 en Python pur** a
résolu le problème ; le contrôle local indique `compiled_extensions False`.
Cette procédure est un dépannage, pas une installation systématique. Arrêter
serveur et worker avant de modifier leur environnement :

```powershell
$env:DISABLE_SQLALCHEMY_CEXT = '1'
try {
    .\.venv-prefect\Scripts\python.exe -m pip install --force-reinstall --no-deps --no-cache-dir --no-binary=SQLAlchemy 'SQLAlchemy==2.1.6'
    if ($LASTEXITCODE -ne 0) { throw 'La reconstruction SQLAlchemy a échoué' }
} finally {
    Remove-Item Env:DISABLE_SQLALCHEMY_CEXT -ErrorAction SilentlyContinue
}
.\.venv-prefect\Scripts\python.exe -c "import sqlalchemy; from sqlalchemy.util import has_compiled_ext; print(sqlalchemy.__version__, has_compiled_ext())"
.\.venv-prefect\Scripts\python.exe -m pip check
.\.venv-prefect\Scripts\prefect.exe version
```

`--no-binary=SQLAlchemy` reconstruit depuis les sources ; `--no-cache-dir`
évite de réutiliser un wheel compilé et `--no-deps` conserve les dépendances
figées. Le drapeau est décrit dans le
[guide SQLAlchemy](https://docs.sqlalchemy.org/en/21/intro.html#building-the-cython-extensions).
Relancer serveur et worker puis vérifier une inbox vide.

## Validation sans ingestion réelle

Les tests utilisent des dossiers temporaires et simulent le sous-processus
métier, sans contacter PostgreSQL ni l'API du serveur en fonctionnement.
Les tests unitaires ne démarrent aucun serveur. Les tests du moteur
Prefect démarrent puis arrêtent un serveur **temporaire** sur un port libre,
avec sa propre base SQLite ; ils vérifient les états réels des flows :

```powershell
.\.venv-prefect\Scripts\python.exe -B -m pytest -p no:cacheprovider tests/unit/test_prefect_orchestration.py -ra
.\.venv-prefect\Scripts\python.exe -B -m pytest -p no:cacheprovider tests/prefect/test_flow_engine.py -ra
.\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -m 'not integration' -ra
.\.venv\Scripts\ruff.exe check --no-cache .
.\.venv\Scripts\ruff.exe format --check --no-cache .
```

Dans `.venv`, les tests Prefect sont ignorés si Prefect est absent : lancer aussi
les deux premières commandes. Les tests PostgreSQL restent réservés à `belgian_energy_test`
avec les cinq variables `TEST_PG*` ; voir le [guide dédié](integration_tests.md).
Ne jamais les diriger vers `belgian_energy`. La recette UI avec un CSV réel et
l'activation du Cron sont des validations manuelles distinctes, à effectuer
seulement après autorisation du traitement.

## Acquisition API quotidienne : elia-api-daily

Le flow `belgian-energy-api` (`src/prefect/api.py:energy_api`) contient une seule
task, `run-api-pipeline`. Elle lance `.venv/Scripts/python.exe -B
src/api_pipeline.py`, attend sa fin et contrôle le JSON **et** le code de sortie.
Le sous-processus conserve la preuve d'acquisition de 9C et réalise acquisition,
validation, ingestion avec `elia-api`, corrections historisées et dbt. Prefect
n'importe aucun module métier et ne reconstitue pas lui-même de reçu fiable.
La [documentation API](elia_api.md) décrit ce parcours métier et ses limites.

Les trois deployments coexistent dans le pool **process** `energy-local`, avec le
même worker. `elia-manual` lit un CSV donné ; `elia-inbox` prend un CSV prêt et le
classe ; `elia-api-daily` acquiert directement les observations et conserve son
CSV sous `data/raw/api/`. Le flow API ne déplace rien vers inbox/processing/archive
et ne crée pas de fichier `.result.json` à côté du CSV. Le JSON et les logs Prefect,
les fichiers acquis et les diagnostics SQL existants assurent la traçabilité.

### Enregistrement et premier lancement manuel

Depuis le terminal d'administration, avec le serveur et le worker existants en
fonctionnement selon les commandes plus haut :

```powershell
Set-Location 'D:\Utilisateurs\Mike\Projets\belgian-energy-data-pipeline'
$env:PREFECT_API_URL = 'http://127.0.0.1:4200/api'
$env:ENERGY_PROJECT_ROOT = (Get-Location).Path

# Enregistrer seulement le nouveau deployment ; ne lance pas le pipeline
.\.venv-prefect\Scripts\prefect.exe deploy --name elia-api-daily --prefect-file prefect.yaml --no-prompt
.\.venv-prefect\Scripts\prefect.exe deployment inspect 'belgian-energy-api/elia-api-daily'
.\.venv-prefect\Scripts\prefect.exe deployment schedule ls 'belgian-energy-api/elia-api-daily'
```

Le YAML déclare `parameters: {}` et une schedule `0 10 * * *`, fuseau
`Europe/Brussels`, **`active: false`**. Aucun enregistrement n'est effectué par le
développement ou les tests. `deploy --all` réenregistre les trois deployments et
remet les deux schedules du YAML à l'état inactif. Inspecter cet état après chaque
réenregistrement. Le type `process` appartient au pool existant ; aucun autre
pool, worker ou service Windows n'est créé.

Pour démarrer les composants s'ils sont arrêtés, utiliser deux terminaux séparés,
après les variables communes indiquées plus haut :

```powershell
# Terminal serveur, seulement s'il n'est pas déjà lancé
.\.venv-prefect\Scripts\prefect.exe server start --host 127.0.0.1 --port 4200

# Terminal worker, seulement s'il n'est pas déjà lancé
.\.venv-prefect\Scripts\prefect.exe worker start --pool energy-local --type process --name energy-local-worker --limit 1
```

Dans <http://127.0.0.1:4200>, ouvrir **Deployments**, choisir
**belgian-energy-api / elia-api-daily**, puis **Run** :

1. Pour une première validation contrôlée, renseigner `start` et `end`, par
   exemple `2025-10-26T00:00:00Z` et `2025-10-26T02:00:00Z` (fin exclusive).
   Ce lancement écrit réellement en staging et lance dbt.
2. Pour la fenêtre courante, laisser les deux valeurs à `null` ou omises. Ne pas
   saisir les chaînes littérales `"null"` ou des chaînes vides.
3. Déclencher puis suivre **Flow Runs**, ouvrir le run et sa task. Attendre
   `Completed` et vérifier le statut métier `SUCCESS` ou `NO_CHANGE`, les batchs
   et les logs dbt avant d'activer l'horaire.

Les deux bornes doivent être fournies ensemble, en ISO 8601 UTC (`Z` ou
`+00:00`). Le flow les transmet directement au client. Sans bornes, il ne passe
aucune option temporelle : `api_pipeline.py` calcule sa fenêtre de 30 jours passés
jusqu'à 8 jours futurs au moment réel d'exécution. Prefect ne recalcule ni cette
fenêtre ni une fenêtre à partir de l'heure théorique du run.

### Activer ou désactiver l'horaire

Après validation manuelle, inspecter la schedule puis l'activer volontairement :

```powershell
.\.venv-prefect\Scripts\prefect.exe deployment schedule ls 'belgian-energy-api/elia-api-daily'
# Ce deployment possède une seule schedule dans prefect.yaml
.\.venv-prefect\Scripts\prefect.exe deployment schedule resume 'belgian-energy-api/elia-api-daily' --all
.\.venv-prefect\Scripts\prefect.exe deployment schedule ls 'belgian-energy-api/elia-api-daily'

# Pour la désactiver ensuite
.\.venv-prefect\Scripts\prefect.exe deployment schedule pause 'belgian-energy-api/elia-api-daily' --all
```

Le bouton de schedule dans l'interface donne le même contrôle. L'heure reste
**10h00 à Bruxelles** lors des changements d'heure : 09h00 UTC en hiver, 08h00 UTC
en été. Ce n'est pas un intervalle fixe de 24 heures en UTC. Désactiver la
schedule ne stoppe pas un traitement actif et peut laisser des runs déjà créés :
les examiner dans l'interface. La schedule inbox reste indépendante.

### Résultats, logs et diagnostic

| Résultat CLI API | Code | État Prefect |
| --- | --- | --- |
| `SUCCESS` | 0 | `Completed` |
| `NO_CHANGE` | 0 | `Completed` |
| `DBT_FAILED` | 1 | `Failed`, staging validé conservé |
| `ACQUISITION_FAILED` | 1 | `Failed`, problème HTTP/réseau d'acquisition |
| `VALIDATION_FAILED` | 1 | `Failed`, réponse API/CSV, intégrité ou données refusées |
| `INGESTION_FAILED` | 1 | `Failed`, exception PostgreSQL |
| `TECHNICAL_ERROR` | 1 | `Failed`, autre exception technique |
| `INTERRUPTED` | 130 | `Failed` si l'enfant retourne ce code |
| JSON vide/invalide/incomplet ou code incompatible | quelconque | `Failed` |

La CLI ajoute aux erreurs un JSON `status`/`error_type`, sans texte d'exception
PostgreSQL ni identifiants. La nouvelle sous-classe
`AcquisitionValidationError` distingue les réponses invalides des erreurs de
transport ; elle reste une `AcquisitionError` pour les appelants Python existants.
Le contrat Python et les contrôles métier sont conservés. Les erreurs de syntaxe
CLI gardent le code 2, que le flow traite comme échec technique.

Les logs affichent le début du lancement, `api_acquisition_started`, puis
`api_acquired` avec le fichier, le compte et l'heure réelle d'acquisition. Le
résumé Prefect indique `api_acquisition_finished`, l'identifiant du batch,
`NEW`/`UNCHANGED`/`CHANGED`, l'identifiant du build dbt et le statut final.
`UNCHANGED` est calculé à partir du compte reçu et des compteurs métier lorsqu'ils
sont disponibles ; les compteurs restent `null` sur certains passages idempotents.
Sans build nécessaire, le résumé dbt indique `UP_TO_DATE`. Les avertissements de
qualité sur `Total Load` récent/futur ne deviennent pas artificiellement des
échecs Prefect.

Le sous-processus est attendu avec capture de ses sorties : les logs métier sont
republiés **à sa fin**, sans streaming en temps réel. Pour un échec après
acquisition, `api_acquired` permet de retrouver le CSV même si le résultat final
ne contient qu'une catégorie d'erreur. Pour un processus tué brutalement, le
résultat ou les derniers logs peuvent manquer ; l'état Prefect peut alors être
`Crashed`/`Cancelled` plutôt que `Failed`. Aucun résultat incomplet ne produit un
succès technique. Les résultats Python ne sont pas persistés par les flows.

```powershell
.\.venv-prefect\Scripts\prefect.exe flow-run ls
$apiFlowRunId = 'REMPLACER_PAR_UUID_DU_RUN'
.\.venv-prefect\Scripts\prefect.exe flow-run inspect $apiFlowRunId
.\.venv-prefect\Scripts\prefect.exe flow-run logs $apiFlowRunId
```

### Reprise, concurrence et disponibilité du PC

Flow et task ont **`retries=0`**. Seul le client d'acquisition effectue ses retries
HTTP bornés ; un incident ne relance pas automatiquement toute l'acquisition.
Après diagnostic et correction de la cause, déclencher volontairement un nouveau
run du deployment. Pour rejouer un périmètre, renseigner les deux bornes UTC.
Après `DBT_FAILED`, staging et les corrections restent conservés ; un nouveau run
utilise le mécanisme existant d'idempotence et de reconstruction dbt manquante.
Une nouvelle acquisition peut contenir des révisions Elia supplémentaires.
La reprise du fichier déjà ingéré via le pipeline manuel reste décrite dans le
guide API. Ne pas remettre les extraits API dans le parcours inbox `elia-full`.

Le worker existant `--limit 1` prend un seul run de deployment à la fois pour les
trois parcours. Des lancements directs hors worker peuvent néanmoins se
chevaucher. Les verrous PostgreSQL communs restent responsables de l'ingestion
et de dbt ; un conflit ou un timeout doit être diagnostiqué puis repris
manuellement. L'acquisition HTTP peut se chevaucher sans supprimer ces garanties.
Aucune nouvelle couche de verrouillage ni limite globale supplémentaire n'est
ajoutée.

Un serveur local seul peut créer des runs planifiés, mais il faut un worker actif
pour les exécuter. Si le worker est indisponible, des runs peuvent rester en
attente ou en retard. Quand le PC est éteint ou en veille, ni acquisition ni
ingestion n'a lieu ; Prefect ne réveille pas le PC. Au redémarrage, inspecter et
annuler les anciens runs non souhaités avant de relancer le worker. Un run retardé
sans bornes utilise la fenêtre courante au moment du lancement, sans garantie de
rattrapage historique au-delà de celle-ci. Aucun service Windows, cloud ou
rafraîchissement automatique de Power BI n'est installé.

La CI existante exécute les nouveaux cas dans les mêmes modules de tests :

```powershell
.\.venv-prefect\Scripts\python.exe -B ci/run_tests.py prefect
.\.venv\Scripts\python.exe -B ci/run_tests.py unit
```

Elle vérifie les trois deployments, les transitions d'heure, les sorties CLI
simulées, et les états réels flow/task avec un serveur Prefect temporaire isolé.
Aucun appel réel à Elia, aucune ingestion PostgreSQL et aucun enregistrement de
deployment sur le serveur utilisateur ne sont nécessaires à ces tests.
