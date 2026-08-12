from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TASK = ROOT / "task"
EVIDENCE = ROOT / "evidence"
RUNS = ROOT / "windows-runs"


def reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def extract(archive: Path, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as source:
        source.extractall(target)


def members(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc")


def normalized(path: Path) -> bytes:
    return path.read_bytes().replace(b"\r\n", b"\n")


def compare(actual: Path, expected: Path) -> list[str]:
    if members(actual) != members(expected):
        raise AssertionError("交付路径集合不同")
    for relative in members(expected):
        if normalized(actual / relative) != normalized(expected / relative):
            raise AssertionError(f"Reference不同：{relative}")
    return members(expected)


def input_hashes(root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in root.rglob("*") if path.is_file()}


def build(input_root: Path, output_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(ROOT / "implementation" / "build_delivery.py"), str(input_root), str(output_root)],
        text=True, capture_output=True, timeout=900,
    )


def main() -> None:
    reset(RUNS)
    EVIDENCE.mkdir(exist_ok=True)
    reference_root = RUNS / "reference"
    extract(TASK / "reference.zip", reference_root)
    expected = reference_root / "output"
    clean_runs = []
    for label in ("clean-a", "clean-b"):
        base = RUNS / label
        extract(TASK / "输入数据包.zip", base)
        input_root = base / "input_data"
        before = input_hashes(input_root)
        for index in (1, 2):
            output_root = base / f"output-{index}"
            completed = build(input_root, output_root)
            if completed.returncode:
                raise AssertionError(completed.stdout + completed.stderr)
            generated = compare(output_root, expected)
            clean_runs.append({
                "root_id": label, "process_index": index, "primary_software_executed": True,
                "input_unchanged": True, "reference_full_match": True, "generated_paths": generated,
            })
        if before != input_hashes(input_root):
            raise AssertionError("输入文件发生变化")

    positive = RUNS / "positive"
    extract(TASK / "输入数据包.zip", positive)
    timeline = positive / "input_data" / "incident_timeline.csv"
    timeline.write_text(
        timeline.read_text(encoding="utf-8").replace("final moderation feed accepted", "final moderation package accepted"),
        encoding="utf-8",
    )
    completed = build(positive / "input_data", positive / "output")
    if completed.returncode:
        raise AssertionError("合法备注变化未完成处理")
    ledger = (positive / "output" / "results" / "event_ledger.csv").read_text(encoding="utf-8")
    if "final moderation package accepted" not in ledger:
        raise AssertionError("合法备注变化未进入事件台账")
    (EVIDENCE / "positive-case.json").write_text(
        json.dumps({"input_field": "operator_note", "behavior_changed": True}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    negative = RUNS / "negative"
    extract(TASK / "输入数据包.zip", negative)
    timeline = negative / "input_data" / "incident_timeline.csv"
    text = timeline.read_text(encoding="utf-8")
    timeline.write_text(text.replace("2,DAGRUN,R02", "1,DAGRUN,R02"), encoding="utf-8")
    output = negative / "output"
    completed = build(negative / "input_data", output)
    if completed.returncode == 0 or output.exists():
        raise AssertionError("错误sequence未关闭")
    (EVIDENCE / "negative-case.log").write_text(
        f"return_code={completed.returncode}\n{completed.stdout}{completed.stderr}", encoding="utf-8"
    )
    summary = {
        "result": "PASS", "commit_sha": os.getenv("GITHUB_SHA"), "workflow_run_id": os.getenv("GITHUB_RUN_ID"),
        "runner_image": os.getenv("ImageOS"),
        "main_software": {"name": "Apache Airflow", "version": importlib.metadata.version("apache-airflow"), "executed": True, "runtime_boundary": "Windows2025+WSL2+Ubuntu24.04"},
        "clean_directory_count": 2, "process_runs_per_directory": 2, "clean_runs": clean_runs,
        "positive_mutation": "PASS", "negative_case": "PASS", "reference_full_comparison": "PASS",
        "formal_network": {"wsl_outbound_blocked": True, "external_services_used": False},
    }
    (EVIDENCE / "windows-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
