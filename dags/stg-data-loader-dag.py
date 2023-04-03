import sys
import logging

from datetime import datetime, timedelta
from pathlib import Path

# airflow
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))
from package.main import STGDataLoader

logger = logging.getLogger("airflow.task")

TASK_DEFAULT_ARGS = {
    "owner": "leonide",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": True,
}

loader = STGDataLoader()


@task(default_args=TASK_DEFAULT_ARGS)
def load_products_data() -> None:
    loader.load_products_data_to_dwh()


@task(default_args=TASK_DEFAULT_ARGS)
def load_customers_data() -> None:
    loader.load_customers_data_to_dwh()


@task(default_args=TASK_DEFAULT_ARGS)
def load_geolocation_data() -> None:
    loader.load_geolocation_data_to_dwh()


@task(default_args=TASK_DEFAULT_ARGS)
def load_sellers_data() -> None:
    loader.load_sellers_data_to_dwh()


@task(default_args=TASK_DEFAULT_ARGS)
def load_reviews_data() -> None:
    loader.load_reviews_data_to_dwh()


@task(default_args=TASK_DEFAULT_ARGS)
def load_payments_data() -> None:
    loader.load_payments_data_to_dwh()


@task(default_args=TASK_DEFAULT_ARGS)
def load_order_items_data() -> None:
    loader.load_order_items_data_to_dwh()


@task(default_args=TASK_DEFAULT_ARGS)
def load_orders_data() -> None:
    loader.load_orders_data_to_dwh()


@task_group()
def load_data() -> None:

    load_products_data()
    load_customers_data()
    load_geolocation_data()
    load_sellers_data()
    load_reviews_data()
    load_payments_data()
    load_order_items_data()
    load_orders_data()


@dag(
    dag_id="stg-data-loader-dag",
    schedule=None,
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

    load = load_data()

    begin = EmptyOperator(task_id="begining")
    end = EmptyOperator(task_id="ending")

    trigger = TriggerDagRunOperator(
        task_id="trigger_downstream_dag",
        trigger_dag_id="dds-data-loader-dag",
        wait_for_completion=False,
        allowed_states=["success"],
        failed_states=["skipped", "failed"],
    )

    chain(
        begin,
        load,
        trigger,
        end,
    )


dag()
