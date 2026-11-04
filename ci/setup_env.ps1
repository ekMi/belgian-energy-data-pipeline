param(
    [Parameter(Mandatory=$true)][string]$Environment,
    [Parameter(Mandatory=$true)][string]$Requirements
)
$ErrorActionPreference = 'Stop'
$bootstrap = 'C:\Program Files\Python313\python.exe'
$python = Join-Path $Environment 'Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) {
    & $bootstrap -m venv $Environment
    if ($LASTEXITCODE -ne 0) { throw 'venv creation failed' }
}
# Compare every declared pin, even when a cached environment already exists.
& $python -B ci/check_requirements.py $Requirements
if ($LASTEXITCODE -ne 0) {
    & $python -m pip install -r $Requirements
    if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
    & $python -B ci/check_requirements.py $Requirements
    if ($LASTEXITCODE -ne 0) { throw 'Dependency versions do not match' }
}
& $python -m pip check
if ($LASTEXITCODE -ne 0) { throw 'Inconsistent Python environment' }
