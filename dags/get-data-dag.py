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
from package.main import DataGetter

logger = logging.getLogger("airflow.task")

TASK_DEFAULT_ARGS = {
    "owner": "leonide",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": True,
}

getter = DataGetter()

#
# DATA PREPARITON TASKS DEFINITION
#
@task(default_args=TASK_DEFAULT_ARGS)
def prepare_customers_data() -> None:
    getter.prepare_customers_data()


@task(default_args=TASK_DEFAULT_ARGS)
def prepare_geolocations_data() -> None:
    getter.prepare_geolocations_data()


@task(default_args=TASK_DEFAULT_ARGS)
def prepare_orders_data() -> None:
    getter.prepare_orders_data()


@task(default_args=TASK_DEFAULT_ARGS)
def prepare_products_data() -> None:
    getter.prepare_products_data()


@task(default_args=TASK_DEFAULT_ARGS)
def prepare_sellers_data() -> None:
    getter.prepare_sellers_data()


@task(default_args=TASK_DEFAULT_ARGS)
def prepare_order_items_data() -> None:
    getter.prepare_order_items_data()


@task(default_args=TASK_DEFAULT_ARGS)
def prepare_order_payments_data() -> None:
    getter.prepare_order_payments_data()


@task(default_args=TASK_DEFAULT_ARGS)
def prepare_order_reviews_data() -> None:
    getter.prepare_order_reviews_data()


#
# DATA DOWNLOADER TASK DEFINITION
#
@task(default_args=TASK_DEFAULT_ARGS)
def get_all_prepared_data() -> None:
    getter.get_all_prepared_data()


#
# TASK GROUP DEFINITION
#
@task_group()
def prepare_data() -> None:

    prepare_customers_data()
    prepare_geolocations_data()
    prepare_orders_data()
    prepare_products_data()
    prepare_sellers_data()
    prepare_order_items_data()
    prepare_order_payments_data()
    prepare_order_reviews_data()


#
# DAG DEFINITION
#
@dag(
    dag_id="get-data-dag",
    schedule="0 1 * * *",
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

    prepare = prepare_data()
    get_all = get_all_prepared_data()

    begin = EmptyOperator(task_id="begining")
    end = EmptyOperator(task_id="ending")

    trigger = TriggerDagRunOperator(
        task_id="trigger_downstream_dag",
        trigger_dag_id="stg-data-loader-dag",
        wait_for_completion=False,
        allowed_states=["success"],
        failed_states=["skipped", "failed"],
    )

    chain(
        begin,
        prepare,
        get_all,
        trigger,
        end,
    )


dag()
