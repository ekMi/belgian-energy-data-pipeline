
# Intégration continue — Jenkins

Le projet utilise Jenkins pour valider automatiquement
le code versionné sur GitHub.

## Contrôles

- Récupération du code depuis GitHub
- Environnement Python 3.13 isolé
- Vérifications Ruff
- Tests unitaires pytest
- Publication des résultats JUnit

## Déclenchement

Jenkins utilise le SCM polling pour détecter
les nouveaux commits sur la branche main.

L'exécution des tests est indépendante du pipeline
de données orchestré par Prefect.
