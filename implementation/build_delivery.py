from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


if len(sys.argv) != 3:
    raise SystemExit("usage: build_delivery.py INPUT_ROOT OUTPUT_ROOT")
input_root = Path(sys.argv[1]).resolve()
output_root = Path(sys.argv[2]).resolve()
template = Path(__file__).resolve().parent / "template_output"

if output_root.exists():
    shutil.rmtree(output_root)
shutil.copytree(template, output_root)

airflow_home = output_root.parent / ".airflow-home"
if airflow_home.exists():
    shutil.rmtree(airflow_home)
airflow_home.mkdir(parents=True)
environment = os.environ.copy()
environment.update(
    {
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{(airflow_home / 'airflow.db').as_posix()}",
        "ALE_INPUT_ROOT": str(input_root),
        "ALE_RESULTS_ROOT": str(output_root / "results"),
    }
)
migrate = subprocess.run(
    [sys.executable, "-m", "airflow", "db", "migrate"],
    env=environment, text=True, capture_output=True, timeout=300,
)
if migrate.returncode:
    shutil.rmtree(output_root, ignore_errors=True)
    raise SystemExit(migrate.stdout + migrate.stderr)
audit = subprocess.run(
    [sys.executable, str(output_root / "tools" / "audit_release.py")],
    env=environment, text=True, capture_output=True, timeout=300,
)
if audit.returncode:
    shutil.rmtree(output_root, ignore_errors=True)
    raise SystemExit(audit.stdout + audit.stderr)
