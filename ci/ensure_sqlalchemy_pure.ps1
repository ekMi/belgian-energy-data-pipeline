param(
    [string]$Environment = '.venv-prefect'
)
$ErrorActionPreference = 'Stop'
$python = Join-Path $Environment 'Scripts\python.exe'
$probe = @'
import sys
try:
    import sqlalchemy
    from sqlalchemy.util import has_compiled_ext
    compiled = has_compiled_ext()
    print('SQLAlchemy', sqlalchemy.__version__, 'compiled_extensions', compiled)
    sys.exit(0 if sqlalchemy.__version__ == '2.1.6' and compiled is False else 1)
except Exception as error:
    print('SQLAlchemy import check failed:', type(error).__name__)
    sys.exit(1)
'@

& $python -B -c $probe
if ($LASTEXITCODE -ne 0) {
    Write-Output 'Rebuilding SQLAlchemy 2.1.6 without compiled extensions'
    $previousDisableCext = $env:DISABLE_SQLALCHEMY_CEXT
    try {
        $env:DISABLE_SQLALCHEMY_CEXT = '1'
        & $python -m pip install --no-binary SQLAlchemy --no-cache-dir --force-reinstall --no-deps 'SQLAlchemy==2.1.6'
        if ($LASTEXITCODE -ne 0) { throw 'SQLAlchemy pure Python installation failed' }
    } finally {
        if ($null -eq $previousDisableCext) {
            Remove-Item Env:DISABLE_SQLALCHEMY_CEXT -ErrorAction SilentlyContinue
        } else {
            $env:DISABLE_SQLALCHEMY_CEXT = $previousDisableCext
        }
    }
    & $python -B -c $probe
    if ($LASTEXITCODE -ne 0) { throw 'SQLAlchemy 2.1.6 must import without compiled extensions' }
} else {
    Write-Output 'SQLAlchemy pure Python installation already conformant; no rebuild'
}
& $python -m pip check
if ($LASTEXITCODE -ne 0) { throw 'Inconsistent Prefect Python environment' }
