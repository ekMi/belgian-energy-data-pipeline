param(
    [ValidateSet("server", "worker")]
    [string]$Role
)

$ErrorActionPreference = "Stop"

# Racine du dépôt, deux niveaux au-dessus du script
$projectRoot = (Resolve-Path "$PSScriptRoot\..\..").Path

Set-Location $projectRoot

# Configuration Prefect
$env:PREFECT_API_URL = "http://127.0.0.1:4200/api"
$env:ENERGY_PROJECT_ROOT = $projectRoot

$prefect = Join-Path $projectRoot ".venv-prefect\Scripts\prefect.exe"

# Journaux locaux, hors du dépôt Git
$logDir = Join-Path $env:LOCALAPPDATA "BelgianEnergy\logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

if ($Role -eq "server") {
    & $prefect server start --host 127.0.0.1 --port 4200 `
        *>> (Join-Path $logDir "prefect-server.log")
}
else {
    & $prefect worker start --pool energy-local `
        *>> (Join-Path $logDir "prefect-worker.log")
}

exit $LASTEXITCODE