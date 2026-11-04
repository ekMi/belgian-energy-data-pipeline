# Acquisition API Elia ODS001

Ce composant autonome télécharge et conserve un export validé. Il ne lance aucune
ingestion, connexion PostgreSQL, correction, exécution dbt ou flow Prefect.

## API et fenêtre

Endpoint CSV :
`https://external-elia.opendatasoft.com/api/explore/v2.1/catalog/datasets/ods001/exports/csv`.

Paramètres encodés par `urllib.parse.urlencode` :

- `where=datetime >= '<début UTC>' AND datetime < '<fin UTC>'` ;
- `timezone=UTC`, `order_by=datetime ASC`, `use_labels=true`.

Le même filtre et `timezone=UTC` sont envoyés à `/records` avec `limit=1` pour lire
`total_count`. Aucun secret ni authentification n'est nécessaire.

Par défaut, les bornes sont le minuit **UTC** du jour d'exécution moins 30 jours et
ce même minuit plus 8 jours (fin exclusive), soit 38 jours. Les changements d'heure
belges n'interviennent pas dans ce calcul. `--start` et `--end` doivent être fournis
ensemble : timestamps ISO 8601 avec `Z` ou `+00:00`, début strictement avant fin.
Une fenêtre explicite fixe le périmètre, pas les valeurs susceptibles d'être
corrigées ultérieurement par Elia.

## Exécution PowerShell

Depuis la racine du dépôt :

```powershell
# Fenêtre glissante par défaut
.\.venv\Scripts\python.exe -B src/acquire_elia.py

# Petite fenêtre historique reproductible
.\.venv\Scripts\python.exe -B src/acquire_elia.py `
  --start '2025-10-26T00:00:00Z' `
  --end '2025-10-26T01:00:00Z' `
  --timeout 60 --max-retries 3

# Lire le résultat structuré et vérifier le hash local
$rawResult = & .\.venv\Scripts\python.exe -B src/acquire_elia.py `
  --start '2025-10-26T00:00:00Z' --end '2025-10-26T01:00:00Z'
if ($LASTEXITCODE -ne 0) { throw 'Échec acquisition Elia' }
$result = $rawResult | ConvertFrom-Json
$result
Get-FileHash -LiteralPath $result.file_path -Algorithm SHA256
```

Le JSON unique sur stdout contient `file_path`, `window_start_utc`,
`window_end_utc`, `extracted_at`, `source_reference` (URL CSV complète avec filtre),
`row_count` et `sha256`. Les dates sont en UTC ; `extracted_at` est l'heure réelle
de fin du téléchargement CSV réussi, jamais une date de publication supposée.
Les erreurs explicites vont sur stderr, avec code de sortie 1 (130 si interruption).
Les erreurs de syntaxe CLI utilisent le code 2.

L'API Python `acquire_elia()` renvoie une dataclass `AcquisitionResult` ;
`to_dict()` donne les valeurs sérialisables en JSON. `opener` et `sleep` sont
injectables pour les tests. Aucune nouvelle dépendance n'est ajoutée.

## Contrôles et conservation

- Statut HTTP 200, type `text/csv` ou `application/json` selon la requête, longueur
  annoncée si présente et JSON de comptage valide.
- En-têtes exacts et ordonnés via `EXPECTED_COLUMNS`, CSV UTF-8 strict, au moins
  une observation, dix champs, résolution `PT15M` et nombres finis via `parse_row()`.
- Mesures NULL représentées par un champ vide acceptées dans les huit colonnes
  numériques. Pour valider les autres champs, le client fournit temporairement
  un zéro aux emplacements vides lors de l'appel au parseur d'ingestion, plus
  strict. Ces valeurs ne sont jamais écrites ; le validateur existant est inchangé.
- Timestamps avec fuseau normalisés en UTC pour les contrôles, alignement sur
  15 minutes, ordre strict, unicité `(timestamp_utc, resolution_code)` et respect
  des bornes inclusives/exclusives.
- Nombre de lignes identique au `total_count` API : toute différence bloque la
  publication, y compris un export coupé entre deux lignes valides.

Chaque requête a un timeout de 60 secondes par opération réseau et jusqu'à trois
retries par défaut : HTTP 429/500/502/503/504, timeout, erreurs réseau et transfert
incomplet. Attentes exponentielles de 1, 2 puis 4 secondes (plafond 60 secondes).
Les autres statuts HTTP, erreurs de validation et certificats invalides échouent
immédiatement. Un retry redémarre son téléchargement depuis zéro. Les paramètres
`timeout`, `max_retries` et `backoff` sont réglables dans l'API Python.

Les octets CSV originaux (BOM et fins de ligne compris) sont conservés sous
`data/raw/api/`, déjà ignoré par Git. Le nom inclut les bornes, l'heure d'acquisition
et un UUID. Après écriture temporaire, synchronisation, validation et SHA-256,
un lien physique publie atomiquement le fichier sans écraser un chemin existant,
puis le temporaire est supprimé. Le système de fichiers doit permettre les liens
physiques (NTFS convient) ; aucun repli autorisant l'écrasement n'est utilisé.
Tout échec avant publication supprime le temporaire sans créer d'export définitif.
Un arrêt brutal du processus peut laisser un temporaire `.ods001_*.tmp`.

## Limites

Les corrections antérieures à la fenêtre ne sont pas détectées. La fraîcheur des
mesures et des prévisions n'est pas garantie. Les appels CSV et `/records` ne
constituent pas un snapshot transactionnel : une évolution du nombre de lignes
entre les deux appels peut entraîner un échec de comptage ; relancer l'acquisition.
L'égalité des comptes ne prouve pas l'identité des valeurs entre ces deux instants.
Les lacunes de publication côté source ne sont pas rejetées si le compte API est
cohérent : aucune couverture de tous les quarts d'heure ni complétude des mesures
n'est imposée. Les timestamps du CSV restent dans leur représentation originale.

La commande `acquire_elia.py` reste limitée au téléchargement. Le point
d'entrée `api_pipeline.py` décrit ci-dessous raccorde explicitement l'acquisition
à l'ingestion et à dbt. Aucune planification, modification Prefect ou Jenkins
n'est introduite. L'ingestion conserve ses exigences sur les valeurs obligatoires.

Tests hors réseau :

```powershell
.\.venv\Scripts\python.exe -B -m pytest tests/unit/test_acquire_elia.py -q
.\.venv\Scripts\python.exe -B -m pytest tests/unit -q
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
```

## Pipeline API → PostgreSQL → dbt (phase 9C)

`src/api_pipeline.py` appelle `acquire_elia()`, vérifie son résultat, puis transmet
le CSV à `local_pipeline.run_pipeline()` avec `input_scope=partial` et
`source_policy=elia-api`. Ce même pipeline assure la prévisualisation,
l'ingestion transactionnelle, puis la reconstruction dbt protégée par verrou.
Il n'existe pas de second mécanisme de merge ou de déclenchement dbt.

### Première exécution contrôlée

Ces commandes écrivent en staging puis reconstruisent les modèles analytiques.
Les protections habituelles sur la cible PostgreSQL et le profil dbt restent
actives. Les variables PG du processus ou le fichier `.env` doivent cibler la
base métier voulue ; les migrations existantes 01–05 doivent déjà être appliquées
et `.venv-dbt`/le profil local dbt disponibles. Aucune migration supplémentaire
n'est nécessaire. Les tests de développement ne doivent jamais cibler cette base.

Depuis la racine, commencer par un petit périmètre explicite ; cet exemple couvre
le changement d'heure automnal en UTC. Choisir la fenêtre à importer avant de
lancer la commande :

```powershell
# Précontrôle existant, sans ingestion ni reconstruction
.\.venv\Scripts\python.exe -B src/dbt_guard.py --check-only
if ($LASTEXITCODE -ne 0) { throw 'Précontrôle dbt refusé' }

# Acquisition, ingestion et dbt : première exécution manuelle
$pipelineJson = & .\.venv\Scripts\python.exe -B src/api_pipeline.py `
  --start '2025-10-26T00:00:00Z' `
  --end '2025-10-26T02:00:00Z'
$pipelineExit = $LASTEXITCODE
if ($pipelineJson) { $pipelineResult = $pipelineJson | ConvertFrom-Json; $pipelineResult }
if ($pipelineExit -ne 0) { throw 'Pipeline incomplet : consulter stderr et les diagnostics du batch' }

# Après validation de ce premier passage : fenêtre glissante 30 jours + 8 jours
.\.venv\Scripts\python.exe -B src/api_pipeline.py
```

Le JSON stdout contient `status`, `acquisition` (toutes les métadonnées de 9B) et
`pipeline` (`ingestion_batch_id`, `target_batch_id`, `build_id`, compteurs et
catégorie éventuelle d'échec dbt). Les logs vont sur stderr. `SUCCESS` et
`NO_CHANGE` donnent un code 0 ; `DBT_FAILED` donne un code 1 avec le JSON du
résultat. Les exceptions techniques passent par la frontière CLI existante,
qui journalise une catégorie sans divulguer les paramètres de connexion.
Un échec d'acquisition ou d'intégrité précède toute lecture/écriture PostgreSQL.

### Politique elia-api et provenance

- `restricted` conserve l'autorisation manuelle liée au hash pour les corrections.
- `elia-full` conserve l'autorisation automatique sur les exports complets
  `continuous` et refuse toujours `partial`.
- `elia-api` autorise les corrections automatiques sur un import `partial`
  **uniquement** avec un reçu d'acquisition émis par `acquire_elia()` dans le même
  processus Python. Il ne se combine pas avec une autorisation manuelle.

Le client enregistre temporairement l'identité de l'objet `AcquisitionResult`
qu'il a réellement émis. Un objet construit ou copié ou rechargé depuis un JSON
n'accorde aucun droit. Le chemin du CSV, ses octets SHA-256, `extracted_at` et
`source_reference` doivent correspondre au reçu. La prévisualisation vérifie le
reçu, l'ingestion vérifie encore le hash sous son verrou et le chargeur le
recontrôle après COPY, avant tout merge. Un CSV arbitraire accompagné de
`--source-policy elia-api` est donc rejeté par les CLI manuelles.

Cette garantie contrôle les entrées applicatives ; elle ne constitue ni une
signature, ni une authentification persistante du fichier. Du code malveillant
exécuté dans le même processus, une machine compromise ou un transport HTTP
remplacé peuvent contourner cette frontière. Le reçu n'est pas une preuve de
publication/fraîcheur chez Elia. Il ne survit pas au redémarrage du processus.
Pour un nouveau passage API, relancer `api_pipeline.py` afin d'acquérir un nouveau
reçu, même si la fenêtre est identique. Le CSV reçu reste conservé sur disque.

### Traçabilité et corrections

Le schéma SQL reste inchangé. `staging.ingestion_batch` enregistre le nom du CSV,
son SHA-256, la référence source complète, l'heure réelle de fin du téléchargement
(`extracted_at`), `input_scope=partial`, `correction_policy=authorized`, le hash
autorisé et la justification :

`Automatic update from validated official Elia ODS001 API acquisition`

La classification `NEW`/`UNCHANGED`/`CHANGED`, les compteurs et
`staging.elia_load_correction` sont ceux de l'ingestion existante. Les anciennes
et nouvelles valeurs sont historisées dans la transaction, y compris NULL ↔
valeur pour `Total Load` et `Week-ahead forecast`. Les lignes identiques conservent
leur provenance. Une ligne absente du CSV ne supprime jamais une ligne en base.
La protection existante rejette les corrections dont `extracted_at` précède une
version connue de la même observation, en tenant aussi compte de son historique.
Elle ne détecte pas une source obsolète si aucune date comparable n'est connue.

Les journaux identifient le fichier acquis avant l'ingestion. Les diagnostics
existants restent consultables par identifiant de batch ; un refus avant la
création du batch n'en crée pas. Le CSV est conservé en cas d'échec de l'ingestion
ou de dbt. Un export identique conserve son nouveau fichier, mais réutilise
le batch `SUCCESS` existant : les logs et le JSON tracent ce nouveau passage.
Le fichier peut être relié aux batchs par `source_file` et `file_sha256`.

### Qualité et reprise

Les contrôles existants sont adaptés au périmètre : l'import partiel n'impose pas
une continuité complète du CSV ; les contrôles consolidés examinent les clés
reçues et leurs vrais voisins, sans inventer de ligne à la fin de l'horizon.
Les lacunes restent signalées comme avertissements dans le consolidé. Les
vérifications de clés, de quart d'heure et de correspondance au contenu reçu
restent bloquantes. Les calculs travaillent sur les instants UTC, y compris lors
des changements d'heure Europe/Brussels.

`Total Load` absent, futur ou publié avec retard, reste un avertissement de
complétude ; il n'empêche pas l'ingestion. Les prévisions présentes sont
contrôlées et leurs corrections historisées. Aucun assouplissement global du
parseur ou du schéma n'est introduit : les six colonnes de prévisions déjà
obligatoires en PostgreSQL restent obligatoires. Un export contenant des NULL
dans ces colonnes peut réussir l'acquisition 9B mais sera refusé par l'ingestion.
Ce cas nécessiterait une évolution de contrat démontrée et distincte ; aucun
zéro fictif n'est inséré en base.

Si dbt échoue après l'ingestion, staging et l'historique restent validés. La
reconstruction est suivie par `staging.dbt_build_run` et le watermark existants.
Relancer la même commande API : un hash déjà `SUCCESS` est ignoré côté ingestion, tandis
que `run_pipeline()` relance la reconstruction manquante sous verrou. Si Elia a
modifié le CSV entre les passages, le merge habituel s'applique d'abord au nouvel
export puis dbt reconstruit la cible courante. On peut aussi reprendre le fichier
déjà ingéré via le pipeline manuel, sans lui attribuer `elia-api` :

```powershell
# Utiliser le chemin de l'acquisition dont le batch est déjà SUCCESS
.\.venv\Scripts\python.exe -B src/local_pipeline.py `
  'data/raw/api/<fichier-deja-ingere>.csv' --input-scope partial
```

Cette dernière commande reste soumise à `restricted` si le fichier a changé.
Aucune intégration Prefect ni planification automatique n'est activée.

Tests du raccordement (HTTP et processus dbt simulés) :

```powershell
.\.venv\Scripts\python.exe -B -m pytest tests/unit/test_api_pipeline.py tests/unit/test_source_policy.py -q
# Configurer exclusivement les TEST_PG* décrits dans docs/integration_tests.md
.\.venv\Scripts\python.exe -B -m pytest tests/integration/test_api_pipeline_postgres.py -q -rs
```
