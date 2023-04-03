import sys
import logging

from datetime import datetime, timedelta
from pathlib import Path

# airflow
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))
from package.main import DWHCreator

logger = logging.getLogger("airflow.task")

TASK_DEFAULT_ARGS = {
    "owner": "leonide",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": True,
}

creator = DWHCreator()


@task(default_args=TASK_DEFAULT_ARGS)
def create_stg_layer() -> None:
    creator.create_stg_layer()


@task(default_args=TASK_DEFAULT_ARGS)
def create_dds_hubs() -> None:
    creator.create_dds_hubs()


@task(default_args=TASK_DEFAULT_ARGS)
def create_dds_links() -> None:
    creator.create_dds_links()


@task(default_args=TASK_DEFAULT_ARGS)
def create_dds_satelities() -> None:
    creator.create_dds_satelities()


@dag(
    dag_id="dwh-creator-dag",
    schedule="@once",
    start_date=datetime(2023, 4, 3),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["online-shop-vertica-dwh"],
    default_args={
        "owner": "leonide",
    },
    default_view="graph",
)
def dag() -> None:

    stg = create_stg_layer()

    hubs = create_dds_hubs()
    links = create_dds_links()
    sats = create_dds_satelities()

    trigger = TriggerDagRunOperator(
        task_id="trigger_downstream_dag",
        trigger_dag_id="get-data-dag",
        wait_for_completion=False,
        allowed_states=["success"],
        failed_states=["skipped", "failed"],
    )

    begin = EmptyOperator(task_id="begining")
    chain_ = EmptyOperator(task_id="chainig")
    end = EmptyOperator(task_id="ending")

    chain(begin, [stg, hubs], chain_, links, sats, trigger, end)


dag()
