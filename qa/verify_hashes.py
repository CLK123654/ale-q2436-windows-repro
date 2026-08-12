from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
expected = json.loads((ROOT / "qa" / "expected_hashes.json").read_text(encoding="utf-8"))
for name, digest in expected.items():
    actual = hashlib.sha256((ROOT / "task" / name).read_bytes()).hexdigest()
    if actual != digest:
        raise SystemExit(f"attachment hash mismatch: {name}")
