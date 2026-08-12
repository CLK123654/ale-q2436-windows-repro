from __future__ import annotations

import csv
import json
import os
import shutil
import sys
from pathlib import Path

import pendulum
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.log import Log
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType


OUTPUT_ROOT = Path(__file__).resolve().parents[1]
INPUT_ROOT = Path(os.environ.get("ALE_INPUT_ROOT", OUTPUT_ROOT.parent / "input_data")).resolve()
RESULTS_ROOT = Path(os.environ.get("ALE_RESULTS_ROOT", OUTPUT_ROOT / "results")).resolve()
STAGE_ROOT = RESULTS_ROOT.parent / f".{RESULTS_ROOT.name}.stage-{os.getpid()}"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def task_graph(dag: DAG) -> tuple[list[str], list[list[str]]]:
    tasks = [task.task_id for task in dag.tasks]
    edges = sorted(
        [task.task_id, downstream]
        for task in dag.tasks
        for downstream in task.downstream_task_ids
    )
    return tasks, edges


def trailing_failures(session, dag_id: str) -> int:
    runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id)
        .order_by(DagRun.execution_date.desc())
        .all()
    )
    count = 0
    for run in runs:
        if run.state != DagRunState.FAILED:
            break
        count += 1
    return count


def pause_count(session, dag_id: str) -> int:
    return session.query(Log).filter(Log.dag_id == dag_id, Log.event == "paused").count()


def load_materials() -> tuple[dict, list[dict[str, str]], DAG]:
    policy = json.loads((INPUT_ROOT / "pause_policy.json").read_text(encoding="utf-8"))
    events = read_csv(INPUT_ROOT / "incident_timeline.csv")
    sequences = [int(row["sequence"]) for row in events]
    if sequences != list(range(1, len(events) + 1)):
        raise ValueError("sequence必须从1连续递增")
    event_ids = [row["event_id"] for row in events]
    if len(event_ids) != len(set(event_ids)) or any(not item for item in event_ids):
        raise ValueError("event_id必须非空且唯一")
    dagbag = DagBag(dag_folder=str(OUTPUT_ROOT / "dags"), include_examples=False, safe_mode=False)
    if dagbag.import_errors:
        raise RuntimeError(str(dagbag.import_errors))
    dag = dagbag.get_dag(policy["dag_id"])
    if dag is None:
        raise RuntimeError("未找到交付DAG")
    tasks, edges = task_graph(dag)
    expected = {
        "schedule": dag.schedule_interval,
        "catchup": dag.catchup,
        "max_active_runs": dag.max_active_runs,
        "max_consecutive_failed_dag_runs": dag.max_consecutive_failed_dag_runs,
        "is_paused_upon_creation": dag.is_paused_upon_creation,
        "expected_tasks": tasks,
        "expected_edges": edges,
    }
    for name, actual in expected.items():
        if policy[name] != actual:
            raise ValueError(f"DAG配置与策略不一致：{name}")
    return policy, events, dag


def run_audit(policy: dict, events: list[dict[str, str]], dag: DAG) -> None:
    if STAGE_ROOT.exists():
        shutil.rmtree(STAGE_ROOT)
    STAGE_ROOT.mkdir(parents=True)
    DAG.bulk_write_to_db({dag})
    ledger = []
    pause_events = []

    with create_session() as session:
        for event in events:
            before_pauses = pause_count(session, dag.dag_id)
            if event["event_type"] == "DAGRUN":
                logical_date = pendulum.parse(event["logical_date"])
                dagrun = dag.create_dagrun(
                    state=DagRunState.RUNNING,
                    execution_date=logical_date,
                    run_id=event["event_id"],
                    start_date=logical_date,
                    external_trigger=True,
                    run_type=DagRunType.MANUAL,
                    data_interval=(logical_date, logical_date.add(days=1)),
                    session=session,
                )
                task_instances = dagrun.get_task_instances(session=session)
                for task_instance in task_instances:
                    task_instance.task = dag.get_task(task_instance.task_id)
                    if event["run_state"] == "failed" and task_instance.task_id == "publish_moderation_feed":
                        task_instance.set_state(TaskInstanceState.FAILED, session=session)
                    else:
                        task_instance.set_state(TaskInstanceState.SUCCESS, session=session)
                session.flush()
                dagrun.update_state(session=session, execute_callbacks=False)
                session.flush()
                if dagrun.state != event["run_state"]:
                    raise RuntimeError(f"DagRun终态不一致：{event['event_id']}")
            elif event["event_type"] == "ADMIN_UNPAUSE":
                model = DagModel.get_dagmodel(dag.dag_id, session=session)
                if model is None:
                    raise RuntimeError("DagModel不存在")
                model.set_is_paused(False, session=session)
            else:
                raise ValueError(f"未知事件类型：{event['event_type']}")

            session.flush()
            session.expire_all()
            model = DagModel.get_dagmodel(dag.dag_id, session=session)
            current_pauses = pause_count(session, dag.dag_id)
            pause_delta = current_pauses - before_pauses
            streak = trailing_failures(session, dag.dag_id)
            ledger.append(
                {
                    **event,
                    "trailing_failures": streak,
                    "is_paused": str(bool(model.is_paused)).lower(),
                    "pause_event_delta": pause_delta,
                }
            )
            if pause_delta:
                pause_events.append(
                    {
                        "ordinal": current_pauses,
                        "triggered_after_event": event["event_id"],
                        "event": "paused",
                        "dag_id": dag.dag_id,
                    }
                )

        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id)
            .order_by(DagRun.execution_date)
            .all()
        )
        history = [
            {
                "run_id": row.run_id,
                "logical_date": row.execution_date.isoformat().replace("+00:00", "Z"),
                "state": row.state,
                "run_type": row.run_type,
                "external_trigger": str(bool(row.external_trigger)).lower(),
            }
            for row in runs
        ]
        model = DagModel.get_dagmodel(dag.dag_id, session=session)
        tasks, edges = task_graph(dag)
        snapshot = {
            "dag_id": dag.dag_id,
            "schedule": dag.schedule_interval,
            "catchup": dag.catchup,
            "max_active_runs": dag.max_active_runs,
            "max_consecutive_failed_dag_runs": dag.max_consecutive_failed_dag_runs,
            "is_paused_upon_creation": dag.is_paused_upon_creation,
            "is_paused": bool(model.is_paused),
            "tasks": tasks,
            "edges": edges,
        }

    write_csv(
        STAGE_ROOT / "event_ledger.csv",
        [
            "sequence", "event_type", "event_id", "logical_date", "run_state", "operator_note",
            "trailing_failures", "is_paused", "pause_event_delta",
        ],
        ledger,
    )
    write_csv(
        STAGE_ROOT / "dagrun_history.csv",
        ["run_id", "logical_date", "state", "run_type", "external_trigger"],
        history,
    )
    write_csv(
        STAGE_ROOT / "pause_events.csv",
        ["ordinal", "triggered_after_event", "event", "dag_id"],
        pause_events,
    )
    (STAGE_ROOT / "dag_snapshot.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if RESULTS_ROOT.exists():
        shutil.rmtree(RESULTS_ROOT)
    STAGE_ROOT.rename(RESULTS_ROOT)


if __name__ == "__main__":
    policy_value, timeline, delivery_dag = load_materials()
    run_audit(policy_value, timeline, delivery_dag)
