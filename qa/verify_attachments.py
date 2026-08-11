from __future__ import annotations

import hashlib
import json
import os
import shutil
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "task"
EVIDENCE = ROOT / "evidence"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def tree(root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): digest(path) for path in sorted(root.rglob("*")) if path.is_file()}


expected = json.loads((ROOT / "qa/expected_hashes.json").read_text(encoding="utf-8"))
actual = {name: digest(TASK / name) for name in expected}
if actual != expected:
    raise SystemExit(f"attachment hash mismatch: {actual}")

if EVIDENCE.exists():
    shutil.rmtree(EVIDENCE)
EVIDENCE.mkdir()
comparisons = {}
for archive in ("输入数据包.zip", "reference.zip"):
    roots = []
    for label in ("clean-a", "clean-b"):
        target = EVIDENCE / label / archive.replace(".zip", "")
        target.mkdir(parents=True)
        with zipfile.ZipFile(TASK / archive) as package:
            package.extractall(target)
        roots.append(target)
    left, right = map(tree, roots)
    if left != right:
        raise SystemExit(f"independent extraction mismatch: {archive}")
    comparisons[archive] = {"files": len(left), "tree_sha256": hashlib.sha256(json.dumps(left, sort_keys=True).encode()).hexdigest()}

payload = {
    "result": "PASS",
    "qid": json.loads((TASK / "task_fields.json").read_text(encoding="utf-8"))["题目ID"],
    "commit_sha": os.getenv("GITHUB_SHA", "local"),
    "workflow_run_id": os.getenv("GITHUB_RUN_ID", "local"),
    "attachment_sha256": actual,
    "independent_clean_directories": comparisons,
}
(EVIDENCE / "attachment_evidence.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
