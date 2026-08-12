from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import datetime


with DAG(
    dag_id="moderation_feed_publish_guard",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_consecutive_failed_dag_runs=3,
    is_paused_upon_creation=False,
) as dag:
    collect_review_decisions = EmptyOperator(task_id="collect_review_decisions")
    verify_policy_coverage = EmptyOperator(task_id="verify_policy_coverage")
    publish_moderation_feed = EmptyOperator(task_id="publish_moderation_feed")

    collect_review_decisions >> verify_policy_coverage >> publish_moderation_feed
