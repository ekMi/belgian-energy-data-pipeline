"""Launch Prefect server or worker without a Windows console."""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PREFECT = ROOT / ".venv-prefect" / "Scripts" / "prefect.exe"

role = sys.argv[1] if len(sys.argv) == 2 else ""

if role not in ("server", "worker"):
    sys.exit(2)

env = os.environ.copy()
env["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
env["ENERGY_PROJECT_ROOT"] = str(ROOT)

if role == "server":
    command = [
        str(PREFECT),
        "server",
        "start",
        "--host",
        "127.0.0.1",
        "--port",
        "4200",
    ]
else:
    command = [
        str(PREFECT),
        "worker",
        "start",
        "--pool",
        "energy-local",
    ]

log_dir = Path(os.environ["LOCALAPPDATA"]) / "BelgianEnergy" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

with (log_dir / f"prefect-{role}.log").open("a", encoding="utf-8") as log:
    log.write(f"\n=== Started {datetime.now().isoformat()} ===\n")
    log.flush()

    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NO_WINDOW,
        check=False,
    )

sys.exit(result.returncode)
