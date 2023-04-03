import sys
import logging

from datetime import datetime, timedelta
from pathlib import Path

# airflow
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))
from package.main import DDSDataLoader

logger = logging.getLogger("airflow.task")

TASK_DEFAULT_ARGS = {
    "owner": "leonide",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": True,
}

loader = DDSDataLoader()

#
# UPDATE HUBS DEFINITION
#
@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_customers() -> None:
    loader.update_hub_customers()


@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_orders() -> None:
    loader.update_hub_orders()


@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_products() -> None:
    loader.update_hub_products()


@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_categories() -> None:
    loader.update_hub_categories()


@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_sellers() -> None:
    loader.update_hub_sellers()


@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_reviews() -> None:
    loader.update_hub_reviews()


@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_geolocations() -> None:
    loader.update_hub_geolocations()


@task(default_args=TASK_DEFAULT_ARGS)
def update_hub_cities() -> None:
    loader.update_hub_cities()


#
# UPDATE LINKS DEFINITION
#
@task(default_args=TASK_DEFAULT_ARGS)
def update_l_order_customer() -> None:
    loader.update_l_order_customer()


@task(default_args=TASK_DEFAULT_ARGS)
def update_l_customer_geolocation() -> None:
    loader.update_l_customer_geolocation()


@task(default_args=TASK_DEFAULT_ARGS)
def update_l_geolocation_city() -> None:
    loader.update_l_geolocation_city()


@task(default_args=TASK_DEFAULT_ARGS)
def update_l_seller_geolocation() -> None:
    loader.update_l_seller_geolocation()


@task(default_args=TASK_DEFAULT_ARGS)
def update_l_product_seller() -> None:
    loader.update_l_product_seller()


@task(default_args=TASK_DEFAULT_ARGS)
def update_l_product_category() -> None:
    loader.update_l_product_category()


@task(default_args=TASK_DEFAULT_ARGS)
def update_l_review_order() -> None:
    loader.update_l_review_order()


@task(default_args=TASK_DEFAULT_ARGS)
def update_l_order_items() -> None:
    loader.update_l_order_items()


#
# UPDATE SATELITIES DEFINITION
#
@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_customers_geolocation() -> None:
    loader.update_sat_customers_geolocation()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_customers_info() -> None:
    loader.update_sat_customers_info()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_customers_contacts() -> None:
    loader.update_sat_customers_contacts()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_reviews() -> None:
    loader.update_sat_reviews()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_category() -> None:
    loader.update_sat_category()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_products_names() -> None:
    loader.update_sat_products_names()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_products_dimensions() -> None:
    loader.update_sat_products_dimensions()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_sellers_info() -> None:
    loader.update_sat_sellers_info()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_order_details() -> None:
    loader.update_sat_order_details()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_order_statuses() -> None:
    loader.update_sat_order_statuses()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_coordinates() -> None:
    loader.update_sat_coordinates()


@task(default_args=TASK_DEFAULT_ARGS)
def update_sat_cities_info() -> None:
    loader.update_sat_cities_info()


#
# TASK GROUPS DEFINITION
#
@task_group()
def update_hubs() -> None:

    update_hub_customers()
    update_hub_orders()
    update_hub_products()
    update_hub_categories()
    update_hub_sellers()
    update_hub_reviews()
    update_hub_geolocations()
    update_hub_cities()


@task_group()
def update_links() -> None:

    update_l_order_customer()
    update_l_customer_geolocation()
    update_l_geolocation_city()
    update_l_seller_geolocation()
    update_l_product_seller()
    update_l_product_category()
    update_l_review_order()
    update_l_order_items()


@task_group()
def update_satelities() -> None:

    update_sat_customers_geolocation()
    update_sat_customers_info()
    update_sat_customers_contacts()
    update_sat_reviews()
    update_sat_category()
    update_sat_products_names()
    update_sat_products_dimensions()
    update_sat_sellers_info()
    update_sat_order_details()
    update_sat_order_statuses()
    update_sat_coordinates()
    update_sat_cities_info()


#
# DAG DEFINITION
#
@dag(
    dag_id="dds-data-loader-dag",
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

    hubs = update_hubs()
    links = update_links()
    sats = update_satelities()

    begin = EmptyOperator(task_id="begining")
    end = EmptyOperator(task_id="ending")

    chain(begin, hubs, links, sats, end)


dag()
