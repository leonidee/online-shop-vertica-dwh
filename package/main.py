import sys
import logging
from pandas import read_csv

from datetime import datetime
import os
import shutil
from pathlib import Path
from faker import Faker

from concurrent.futures import ThreadPoolExecutor

from vertica_python.vertica.cursor import Cursor


# for testing
from tabulate import tabulate

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))

from package.utils import Connector, get_dev_logger
from package.errors import S3ServiceError, FileSystemError, VerticaError

# gets airflow default logger and use it
# logger = logging.getLogger("airflow.task")
logger = get_dev_logger(logger_name=str(Path(Path(__file__).name)))


class DWHCreator:
    def __init__(self) -> None:
        self.dwh_conn = Connector(type="dwh").connect()
        self.path_to_sql = Path(Path.cwd(), "sql")

    def create_stg_layer(self) -> None:
        logger.info("Initializing STG layer.")

        SQL = "stg/stg-ddl"

        try:
            logger.info(f"Reading `{SQL}.sql`.")
            query = Path(self.path_to_sql, f"{SQL}.sql").read_text(encoding="UTF-8")
            logger.info(f"`{SQL}.sql` loaded.")
        except Exception:
            logger.exception(
                f"Unable to read `{SQL}.sql`! Initializing process failed."
            )
            raise FileSystemError

        try:
            logger.info(f"Executing DDL query for STG layer.")

            cur = self.dwh_conn.cursor()
            cur.execute(query)

            logger.info(f"STG layer created successfully.")
        except Exception:
            logger.exception(
                f"Unable to execute DDL query! Initializing process failed."
            )
            raise VerticaError

    def create_dds_hubs(self) -> None:
        logger.info("Initializing DDS layer.")
        logger.info("Creating Hubs.")

        SQL = "dds/hubs-ddl"

        try:
            logger.info(f"Reading `{SQL}.sql`.")
            query = Path(self.path_to_sql, f"{SQL}.sql").read_text(encoding="UTF-8")
            logger.info(f"`{SQL}.sql` loaded.")
        except Exception:
            logger.exception(
                f"Unable to read `{SQL}.sql`! Initializing process failed."
            )
            raise FileSystemError

        try:
            logger.info(f"Executing DDL query for Hubs.")

            cur = self.dwh_conn.cursor()
            cur.execute(query)

            logger.info(f"Hubs of DDS layer were created successfully.")
        except Exception:
            logger.exception(
                f"Unable to execute DDL query! Initializing process failed."
            )
            raise VerticaError


class DataGetter:
    def __init__(self) -> None:
        self.s3_conn = Connector(type="ice-lake").connect()
        self.path_to_data = Path(Path.cwd(), "data")
        self.faker = Faker(locale="pt_BR")
        self._check_data_folder()

    def _check_data_folder(self):
        if not Path.exists(self.path_to_data):
            Path.mkdir(self.path_to_data)

    def _download_file_from_s3(self, folder: str, file: str) -> None:

        KEY = f"e-commerce-data/{folder}/{file}.csv"
        S3_BUCKET = "data-ice-lake-04"

        try:
            self.s3_conn.download_file(
                Filename=Path(self.path_to_data, f"{file}.csv"),
                Bucket=S3_BUCKET,
                Key=KEY,
            )
            logger.info(f"Successfully downloaded `{file}` file.")
        except Exception:
            logger.exception(f"Unable to download `{file}` from s3!")
            raise S3ServiceError

    def _upload_file_to_s3(self, file: str) -> None:

        KEY = f"e-commerce-data/prepared-data/{file}.csv"
        S3_BUCKET = "data-ice-lake-04"

        logger.info(f"Uploading `{file}` to s3.")

        try:
            self.s3_conn.upload_file(
                Filename=Path(self.path_to_data, f"{file}.csv"),
                Bucket=S3_BUCKET,
                Key=KEY,
            )
            logger.info(f"Successfully uploaded `{file}` file to s3.")
        except Exception:
            logger.exception(f"Unable to upload `{file}` to s3!")
            raise S3ServiceError

    def _delete_file(self, file: str) -> None:
        try:
            logger.info(f"Removing `{file}`.")
            os.remove(path=Path(self.path_to_data, f"{file}.csv"))
        except Exception:
            logger.exception(f"Unable to remove `{file}`!")
            raise FileSystemError

    def prepare_customers_data(self) -> None:
        FILE_NAME = "olist_customers_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["customer_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["customer_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        df.customer_city = df.customer_city.str.title()

        try:
            logger.info("Generating additional data.")
            df["customer_name"] = [self.faker.name() for _ in range(df.shape[0])]
            df["customer_email"] = [self.faker.email() for _ in range(df.shape[0])]
            df["phone_number"] = [self.faker.phone_number() for _ in range(df.shape[0])]
            df["date_of_birth"] = [
                str(self.faker.date_of_birth()) for _ in range(df.shape[0])
            ]

        except Exception:
            logger.exception("Unable to generate data and add it to DataFrame!")
            raise Exception

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_geolocations_data(self) -> None:
        FILE_NAME = "olist_geolocation_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if (
            df.duplicated(
                subset=[
                    "geolocation_zip_code_prefix",
                    "geolocation_lat",
                    "geolocation_lng",
                ],
                keep=False,
            ).sum()
            > 0
        ):
            df = df.drop_duplicates(
                subset=[
                    "geolocation_zip_code_prefix",
                    "geolocation_lat",
                    "geolocation_lng",
                ],
                keep=False,
            )
            logger.info(f"Found duplicated values. Dropping.")

        df.geolocation_city = df.geolocation_city.str.title()

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_orders_data(self) -> None:
        FILE_NAME = "olist_orders_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["order_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["order_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        df.order_status = df.order_status.str.capitalize()

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_products_data(self) -> None:
        FILE_NAME = "olist_products_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["product_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["product_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        transl = read_csv(
            "https://storage.yandexcloud.net/data-ice-lake-04/e-commerce-data/raw-data/product_category_name_translation.csv"
        )
        df = df.merge(transl, how="left", on="product_category_name")

        df = df.drop(
            columns=[
                "product_name_lenght",
                "product_description_lenght",
                "product_photos_qty",
            ]
        )
        df.product_category_name = df.product_category_name.str.replace(
            "_", " "
        ).str.capitalize()
        df.product_category_name_english = df.product_category_name_english.str.replace(
            "_", " "
        ).str.capitalize()

        try:
            logger.info("Generating additional data.")
            df["product_name"] = [
                (
                    self.faker.word()
                    + " "
                    + self.faker.word()
                    + " "
                    + self.faker.word()
                ).title()
                for _ in range(df.shape[0])
            ]
        except Exception:
            logger.exception("Unable to generate data and add it to DataFrame!")
            raise Exception

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_sellers_data(self) -> None:
        FILE_NAME = "olist_sellers_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["seller_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["seller_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        df.seller_city = df.seller_city.str.title()

        try:
            df["email"] = [self.faker.email() for _ in range(df.shape[0])]
            df["phone_number"] = [self.faker.phone_number() for _ in range(df.shape[0])]
            logger.info("Generating additional data.")

        except Exception:
            logger.exception("Unable to generate data and add it to DataFrame!")
            raise Exception

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_order_items_data(self) -> None:
        FILE_NAME = "olist_order_items_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)
        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_order_payments_data(self) -> None:
        FILE_NAME = "olist_order_payments_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))
        df.payment_type = df.payment_type.str.replace("_", " ").str.title()

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def prepare_order_reviews_data(self) -> None:
        FILE_NAME = "olist_order_reviews_dataset"
        FOLDER = "raw-data"

        logger.info(f"Preparing `{FILE_NAME}` data.")

        self._download_file_from_s3(folder=FOLDER, file=FILE_NAME)

        df = read_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"))

        if df.duplicated(subset=["review_id"], keep=False).sum() > 0:
            df = df.drop_duplicates(subset=["review_id"], keep=False)
            logger.info(f"Found duplicated values. Dropping.")

        df["is_commented"] = True
        df.loc[df["review_comment_message"].isna(), "is_commented"] = False

        try:
            logger.info(f"Writing file to `{self.path_to_data}` folder.")
            df.to_csv(Path(self.path_to_data, f"{FILE_NAME}.csv"), index=False)
        except Exception:
            logger.exception("Unable to write file!")
            raise FileSystemError

        self._upload_file_to_s3(file=FILE_NAME)
        self._delete_file(file=FILE_NAME)

    def get_all_prepared_data(self) -> None:
        FILE_LIST = [
            "olist_customers_dataset",
            "olist_geolocation_dataset",
            "olist_order_items_dataset",
            "olist_order_payments_dataset",
            "olist_order_reviews_dataset",
            "olist_orders_dataset",
            "olist_products_dataset",
            "olist_sellers_dataset",
        ]
        FOLDER = "prepared-data"

        with ThreadPoolExecutor() as executor:
            [
                executor.submit(self._download_file_from_s3, FOLDER, file)
                for file in FILE_LIST
            ]


class STGDataLoader:
    def __init__(self) -> None:
        self.dwh_conn = Connector(type="dwh").connect()
        self.path_to_data = Path(Path.cwd(), "data")
        self.path_to_sql = Path(Path.cwd(), "sql")

    def _create_temp_table(self, cur: Cursor, sql: str) -> None:

        try:
            logger.info("Creating temp table.")
            cur.execute(sql)

            logger.info("Temp table created.")
        except Exception:
            logger.exception("Unable to create temp table!")
            raise VerticaError

    def _bulk_insert_into_temp_table(
        self, cur: Cursor, sql: str, check_clause: str = None
    ) -> None:

        try:
            logger.info("Inserting data into temp table.")

            cur.execute(sql)

            if check_clause:
                rows_num = cur.execute(check_clause).fetchone()
                logger.info(
                    f"Successfully inserted data into temp table with {rows_num} rows."
                )
            else:
                logger.info(f"Successfully inserted data into temp table.")

        except Exception:
            logger.exception("Unable to insert data into temp table!")
            raise VerticaError

    def _bulk_insert_into_stg_table(
        self, cur: Cursor, sql: str, check_clause: str = None
    ) -> None:

        try:
            logger.info("Bulk insert into stage table.")

            cur.execute(sql)

            if check_clause:
                rows_num = cur.execute(check_clause).fetchone()
                logger.info(
                    f"Successfully inserted data into stage table with {rows_num} rows."
                )
            else:
                logger.info(f"Successfully inserted data into stage table.")

        except Exception:
            logger.exception("Unable to do bulk insert into stage table!")
            raise VerticaError

    def _update_stg_table(self, cur: Cursor, sql: str) -> None:

        try:
            logger.info("Updating stage table.")
            cur.execute(sql)
            logger.info(f"Stage table was successfully updated.")
        except Exception:
            logger.exception("Unable to update stage table!")
            raise VerticaError

    def _drop_temp_table(self, cur: Cursor, sql: str):

        try:
            cur.execute(sql)
            logger.info("Temp table dropped.")
        except Exception:
            logger.exception("Unable to drop temp table!")
            raise VerticaError

    def load_products_data_to_dwh(self) -> None:

        FILE_NAME = "olist_products_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS products_tmp;
                    CREATE LOCAL TEMP TABLE products_tmp
                    (
                        product_id        varchar(100) NOT NULL PRIMARY KEY ENABLED,
                        category_name     varchar(100),
                        weight_g          float,
                        length_cm         float,
                        height_cm         float,
                        width_cm          float,
                        category_name_eng varchar(100),
                        product_name      varchar(100)
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY products_tmp
                    FROM LOCAL '{PATH_TO_FILE}'
                    DELIMITER ','
                    ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM products_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.products AS tgt
                    USING products_tmp AS src
                    ON tgt.product_id = src.product_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET category_name     = src.category_name,
                            category_name_eng = src.category_name_eng,
                            product_name      = src.product_name,
                            weight_g          = src.weight_g,
                            length_cm         = src.length_cm,
                            height_cm         = src.height_cm,
                            width_cm          = src.width_cm
                    WHEN NOT MATCHED THEN
                        INSERT (product_id, category_name, category_name_eng, product_name, weight_g, length_cm, height_cm, width_cm)
                        VALUES (src.product_id, src.category_name, src.category_name_eng, src.product_name, src.weight_g, src.length_cm, src.height_cm,
                                src.width_cm);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS products_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_customers_data_to_dwh(self) -> None:

        FILE_NAME = "olist_customers_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS customers_tmp;
                    CREATE LOCAL TEMP TABLE customers_tmp
                    (
                        customer_id        varchar(100) NOT NULL PRIMARY KEY ENABLED,
                        customer_unique_id varchar(100),
                        zip_code           int,
                        city               varchar(100),
                        state              varchar(50),
                        customer_name      varchar(100),
                        email              varchar(100),
                        phone_number       varchar(100),
                        date_of_birth      date
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY customers_tmp
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM customers_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.customers AS tgt
                    USING customers_tmp AS src
                    ON tgt.customer_id = src.customer_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET customer_unique_id = src.customer_unique_id,
                            customer_name      = src.customer_name,
                            email              = src.email,
                            phone_number       = src.phone_number,
                            date_of_birth      = src.date_of_birth,
                            zip_code           = src.zip_code,
                            city               = src.city,
                            state              = src.state
                    WHEN NOT MATCHED THEN
                        INSERT (customer_id, customer_unique_id, customer_name, email, phone_number, date_of_birth, zip_code, city, state)
                        VALUES (src.customer_id, src.customer_unique_id, src.customer_name, src.email, src.phone_number, src.date_of_birth, src.zip_code,
                                src.city, src.state);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS customers_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_geolocation_data_to_dwh(self) -> None:

        FILE_NAME = "olist_geolocation_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS geolocation_tmp;
                    CREATE LOCAL TEMP TABLE geolocation_tmp
                    (
                        zip_code  int NOT NULL,
                        latitude  float,
                        longitude float,
                        city      varchar(100),
                        state     varchar(50)
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY geolocation_tmp
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM geolocation_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation AS tgt
                    USING geolocation_tmp AS src
                    ON tgt.zip_code = src.zip_code
                    WHEN MATCHED THEN
                        UPDATE
                        SET latitude  = src.latitude,
                            longitude = src.longitude,
                            city      = src.city,
                            state     = src.state
                    WHEN NOT MATCHED THEN
                        INSERT (zip_code, latitude, longitude, city, state)
                        VALUES (src.zip_code, src.latitude, src.longitude, src.city, src.state);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS geolocation_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_sellers_data_to_dwh(self) -> None:

        FILE_NAME = "olist_sellers_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS sellers_tmp;
                    CREATE LOCAL TEMP TABLE sellers_tmp
                    (
                        seller_id    varchar(100) NOT NULL PRIMARY KEY ENABLED,
                        zip_code     int,
                        city         varchar(100),
                        state        varchar(50),
                        email        varchar(100),
                        phone_number varchar(100)
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY sellers_tmp 
                    FROM LOCAL '{PATH_TO_FILE}'
                    DELIMITER ','
                    ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM sellers_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.sellers AS tgt
                    USING sellers_tmp AS src
                    ON tgt.seller_id = src.seller_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET zip_code     = src.zip_code,
                            city         = src.city,
                            state        = src.state,
                            email        = src.email,
                            phone_number = src.phone_number
                    WHEN NOT MATCHED THEN
                        INSERT (seller_id, zip_code, city, state, email, phone_number)
                        VALUES (src.seller_id, src.zip_code, src.city, src.state, src.email, src.phone_number);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS sellers_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_reviews_data_to_dwh(self) -> None:

        FILE_NAME = "olist_order_reviews_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS reviews_tmp;
                    CREATE LOCAL TEMP TABLE reviews_tmp
                    (
                        review_id        varchar(100) NOT NULL PRIMARY KEY ENABLED,
                        order_id         varchar(100),
                        review_score     int,
                        comment_title    varchar(100),
                        comment_message  varchar(300),
                        creation_date    timestamp,
                        answer_timestamp timestamp,
                        is_commented     boolean
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY reviews_tmp
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                    FROM reviews_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.reviews AS tgt
                    USING reviews_tmp AS src
                    ON tgt.review_id = src.review_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET order_id         = src.order_id,
                            review_score     = src.review_score,
                            comment_title    = src.comment_title,
                            comment_message  = src.comment_message,
                            creation_date    = src.creation_date,
                            answer_timestamp = src.answer_timestamp,
                            is_commented     = src.is_commented
                    WHEN NOT MATCHED THEN
                        INSERT (review_id, order_id, review_score, comment_title, comment_message, creation_date, answer_timestamp, is_commented)
                        VALUES (src.review_id, src.order_id, src.review_score, src.comment_title, src.comment_message, src.creation_date, src.answer_timestamp, src.is_commented);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS reviews_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_payments_data_to_dwh(self) -> None:

        FILE_NAME = "olist_order_payments_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._bulk_insert_into_stg_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.payments
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM LEONIDGRISHENKOVYANDEXRU__STAGING.payments;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_order_items_data_to_dwh(self) -> None:

        FILE_NAME = "olist_order_items_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._bulk_insert_into_stg_table(
                cur=cur,
                sql=f"""
                    COPY LEONIDGRISHENKOVYANDEXRU__STAGING.order_items
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM LEONIDGRISHENKOVYANDEXRU__STAGING.order_items;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")

    def load_orders_data_to_dwh(self) -> None:

        FILE_NAME = "olist_orders_dataset"
        PATH_TO_FILE = Path(self.path_to_data, f"{FILE_NAME}.csv")

        logger.info(f"Starting data loading process for `{FILE_NAME}`.")

        try:
            cur = self.dwh_conn.cursor()

            self._create_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS orders_tmp;
                    CREATE LOCAL TEMP TABLE orders_tmp
                    (
                        order_id                      varchar(100) NOT NULL PRIMARY KEY ENABLED,
                        customer_id                   varchar(100),
                        order_status                  varchar(50),
                        order_purchase_timestamp      timestamp,
                        order_approved_at             timestamp,
                        order_delivered_carrier_date  timestamp,
                        order_delivered_customer_date timestamp,
                        order_estimated_delivery_date timestamp
                    )
                        ON COMMIT PRESERVE ROWS
                        DIRECT
                        UNSEGMENTED ALL NODES
                        INCLUDE SCHEMA PRIVILEGES;
                """,
            )
            self._bulk_insert_into_temp_table(
                cur=cur,
                sql=f"""
                    COPY orders_tmp
                        FROM LOCAL '{PATH_TO_FILE}'
                        DELIMITER ','
                        ENCLOSED BY '"';
                """,
                check_clause="""
                    SELECT count(*)
                        FROM orders_tmp;
                """,
            )
            self._update_stg_table(
                cur=cur,
                sql="""
                    MERGE INTO LEONIDGRISHENKOVYANDEXRU__STAGING.orders AS tgt
                    USING orders_tmp AS src
                    ON tgt.order_id = src.order_id
                    WHEN MATCHED THEN
                        UPDATE
                        SET
                            customer_id                   = src.customer_id,
                            order_status                  = src.order_status,
                            order_purchase_timestamp      = src.order_purchase_timestamp,
                            order_approved_at             = src.order_approved_at,
                            order_delivered_carrier_date  = src.order_delivered_carrier_date,
                            order_delivered_customer_date = src.order_delivered_customer_date,
                            order_estimated_delivery_date = src.order_estimated_delivery_date
                    WHEN NOT MATCHED THEN
                        INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
                                order_delivered_customer_date, order_estimated_delivery_date)
                        VALUES (src.order_id, src.customer_id, src.order_status, src.order_purchase_timestamp, src.order_approved_at,
                                src.order_delivered_carrier_date, src.order_delivered_customer_date, src.order_estimated_delivery_date);
                """,
            )
            self._drop_temp_table(
                cur=cur,
                sql="""
                    DROP TABLE IF EXISTS orders_tmp;
                """,
            )
            logger.info(f"Loading process for `{FILE_NAME}` complited succesfully.")
        except Exception:
            logger.exception(f"Loading process failed for `{FILE_NAME}`!")


class DDSDataLoader:
    def __init__(self) -> None:
        self.dwh_conn = Connector(type="dwh").connect()
        self.log = logger.info("Starting updating process for DDS layer.")

    def update_hub_customers(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_customers"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_customers_tmp;
            CREATE LOCAL TEMP TABLE hub_customers_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hash(customer_id)            AS id,
                customer_id,
                '{LOAD_DTTM}'::timestamp(0)  AS load_dttm,
                '{LOAD_SOURCE}'              AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.customers;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers AS tgt
            USING hub_customers_tmp AS src
            ON tgt.id = src.id
            WHEN MATCHED THEN
                UPDATE
                SET customer_id = src.customer_id,
                    load_dttm   = src.load_dttm,
                    load_src    = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (id, customer_id, load_dttm, load_src)
                VALUES (src.id, src.customer_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_customers_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_hub_orders(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_orders"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_orders_tmp;
            CREATE LOCAL TEMP TABLE hub_orders_tmp ON COMMIT PRESERVE ROWS AS
            SELECT hash(order_id)   AS id,
                order_id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.orders;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders AS tgt
            USING hub_orders_tmp AS src
            ON tgt.id = src.id
            WHEN MATCHED THEN
                UPDATE
                SET order_id  = src.order_id,
                    load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (id, order_id, load_dttm, load_src)
                VALUES (src.id, src.order_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_orders_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_hub_products(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_products"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_products_tmp;
            CREATE LOCAL TEMP TABLE hub_products_tmp ON COMMIT PRESERVE ROWS AS
            SELECT hash(product_id) AS id,
                product_id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.products;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_products AS tgt
            USING hub_products_tmp AS src
            ON tgt.id = src.id
            WHEN MATCHED THEN
                UPDATE
                SET product_id = src.product_id,
                    load_dttm  = src.load_dttm,
                    load_src   = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (id, product_id, load_dttm, load_src)
                VALUES (src.id, src.product_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_products_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_hub_categories(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_categories"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_categories_tmp;
            CREATE LOCAL TEMP TABLE hub_categories_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                DISTINCT hash(category_name_eng) AS id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.products;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_categories AS tgt
            USING hub_categories_tmp AS src
            ON tgt.id = src.id
            WHEN NOT MATCHED THEN
                INSERT (id, load_dttm, load_src)
                VALUES (src.id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_categories_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_hub_sellers(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_sellers"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_sellers_tmp;
            CREATE LOCAL TEMP TABLE hub_sellers_tmp ON COMMIT PRESERVE ROWS AS
            SELECT hash(seller_id)  AS id,
                seller_id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.sellers;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers AS tgt
            USING hub_sellers_tmp AS src
            ON tgt.id = src.id
            WHEN MATCHED THEN
                UPDATE
                SET seller_id = src.seller_id,
                    load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (id, seller_id, load_dttm, load_src)
                VALUES (src.id, src.seller_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_sellers_tmp;
         """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_hub_reviews(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_reviews"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_reviews_tmp;
            CREATE LOCAL TEMP TABLE hub_reviews_tmp ON COMMIT PRESERVE ROWS AS
            SELECT hash(review_id)  AS id,
                review_id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.reviews;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_reviews AS tgt
            USING hub_reviews_tmp AS src
            ON tgt.id = src.id
            WHEN MATCHED THEN
                UPDATE
                SET review_id = src.review_id,
                    load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (id, review_id, load_dttm, load_src)
                VALUES (src.id, src.review_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_reviews_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_hub_geolocations(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_geolocations"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_geolocations_tmp;
            CREATE LOCAL TEMP TABLE hub_geolocations_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hash(zip_code, latitude, longitude) AS id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_geolocations AS tgt
            USING hub_geolocations_tmp AS src
            ON tgt.id = src.id
            WHEN NOT MATCHED THEN
                INSERT (id, load_dttm, load_src)
                VALUES (src.id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_geolocations_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_hub_cities(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "hub_cities"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS hub_cities_tmp;
            CREATE LOCAL TEMP TABLE hub_cities_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                DISTINCT hash(city, state) AS id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.hub_cities AS tgt
            USING hub_cities_tmp AS src
            ON tgt.id = src.id
            WHEN NOT MATCHED THEN
                INSERT (id, load_dttm, load_src)
                VALUES (src.id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS hub_cities_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_order_customer(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_order_customer"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            START TRANSACTION;
            DROP TABLE IF EXISTS l_order_customer_tmp;
            CREATE LOCAL TEMP TABLE l_order_customer_tmp ON COMMIT PRESERVE ROWS AS
            SELECT ho.id               AS order_id,
                hc.id               AS customer_id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.orders o
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders ho ON o.order_id = ho.order_id
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers hc ON o.customer_id = hc.customer_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_order_customer AS tgt
            USING l_order_customer_tmp AS src
            ON tgt.customer_id = src.customer_id AND tgt.order_id = src.order_id
            WHEN MATCHED THEN
                UPDATE
                SET load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id, load_dttm, load_src)
                VALUES (src.order_id, src.customer_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_order_customer_tmp;
            END TRANSACTION;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)

            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_customer_geolocation(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_customer_geolocation"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            START TRANSACTION;
            DROP TABLE IF EXISTS l_customer_geolocation_tmp;
            CREATE LOCAL TEMP TABLE l_customer_geolocation_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hc.id AS customer_id,
                c.zip_code,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.customers c
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers hc ON c.customer_id = hc.customer_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_customer_geolocation AS tgt
            USING l_customer_geolocation_tmp AS src
            ON tgt.customer_id = src.customer_id
            WHEN MATCHED THEN
                UPDATE
                SET zip_code  = src.zip_code,
                    load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (customer_id, zip_code, load_dttm, load_src)
                VALUES (src.customer_id, src.zip_code, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_customer_geolocation_tmp;
            END TRANSACTION;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_geolocation_city(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_geolocation_city"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            START TRANSACTION;
            DROP TABLE IF EXISTS l_geolocation_city_tmp;
            CREATE LOCAL TEMP TABLE l_geolocation_city_tmp ON COMMIT PRESERVE ROWS AS
            SELECT DISTINCT hash(hc.id, g.zip_code) AS id,
                            hc.id                   AS city_id,
                            g.zip_code,
                            '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                            '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.geolocation g
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_cities hc ON hash(g.city, g.state) = hc.id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_geolocation_city AS tgt
            USING l_geolocation_city_tmp AS src
            ON tgt.id = src.id
            WHEN MATCHED THEN
                UPDATE
                SET city_id   = src.city_id,
                    zip_code  = src.zip_code,
                    load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (id, city_id, zip_code, load_dttm, load_src)
                VALUES (src.id, src.city_id, src.zip_code, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_geolocation_city_tmp;
            END TRANSACTION;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_seller_geolocation(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_seller_geolocation"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            START TRANSACTION;
            DROP TABLE IF EXISTS l_seller_geolocation_tmp;
            CREATE LOCAL TEMP TABLE l_seller_geolocation_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hs.id AS seller_id,
                s.zip_code,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.sellers s
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers hs ON s.seller_id = hs.seller_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_seller_geolocation AS tgt
            USING l_seller_geolocation_tmp AS src
            ON tgt.seller_id = src.seller_id
            WHEN MATCHED THEN
                UPDATE
                SET zip_code  = src.zip_code,
                    load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (seller_id, zip_code, load_dttm, load_src)
                VALUES (src.seller_id, src.zip_code, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_seller_geolocation_tmp;
            END TRANSACTION;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_product_seller(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_product_seller"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            START TRANSACTION;
            DROP TABLE IF EXISTS l_product_seller_tmp;
            CREATE LOCAL TEMP TABLE l_product_seller_tmp ON COMMIT PRESERVE ROWS AS
            SELECT DISTINCT hs.id AS seller_id,
                            hp.id AS product_id,
                            '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                            '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.order_items oi
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_products hp ON oi.product_id = hp.product_id
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers hs ON oi.seller_id = hs.seller_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_product_seller AS tgt
            USING l_product_seller_tmp AS src
            ON hash(tgt.seller_id, tgt.product_id) = hash(src.seller_id, src.product_id)
            WHEN MATCHED THEN
                UPDATE
                SET load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (seller_id, product_id, load_dttm, load_src)
                VALUES (src.seller_id, src.product_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_product_seller_tmp;
            END TRANSACTION;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_product_category(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_product_category"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            START TRANSACTION;
            DROP TABLE IF EXISTS l_product_category_tmp;
            CREATE LOCAL TEMP TABLE l_product_category_tmp ON COMMIT PRESERVE ROWS AS
            SELECT DISTINCT hp.id AS product_id,
                            hc.id AS category_id,
                            '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                            '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.products p
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_categories hc ON hash(p.category_name_eng) = hc.id
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_products hp ON p.product_id = hp.product_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_product_category AS tgt
            USING l_product_category_tmp AS src
            ON hash(tgt.product_id, tgt.category_id) = hash(src.product_id, src.category_id)
            WHEN MATCHED THEN
                UPDATE
                SET load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (product_id, category_id, load_dttm, load_src)
                VALUES (src.product_id, src.category_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_product_category_tmp;
            END TRANSACTION;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_review_order(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_review_order"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            START TRANSACTION;

            DROP TABLE IF EXISTS l_review_order_tmp;

            CREATE LOCAL TEMP TABLE l_review_order_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hr.id as review_id,
                ho.id as order_id,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.reviews r
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders ho ON r.order_id = ho.order_id
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_reviews hr ON r.review_id = hr.review_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_review_order AS tgt
            USING l_review_order_tmp AS src
            ON hash(tgt.review_id, tgt.order_id) = hash(src.review_id, src.order_id)
            WHEN MATCHED THEN
                UPDATE
                SET load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (review_id, order_id, load_dttm, load_src)
                VALUES (src.review_id, src.order_id, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_review_order_tmp;

            END TRANSACTION;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_l_order_items(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "l_order_items"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS l_order_items_tmp;

            CREATE LOCAL TEMP TABLE l_order_items_tmp ON COMMIT PRESERVE ROWS AS
            SELECT hash(hp.id, ho.id, oi.order_item_id) AS order_item_id,
                hp.id                                AS product_id,
                ho.id                                AS order_id,
                oi.order_item_id                     AS order_item_num,
                oi.price,
                oi.freight_value,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.order_items oi
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_orders ho ON oi.order_id = ho.order_id
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_products hp ON oi.product_id = hp.product_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.l_order_items AS tgt
            USING l_order_items_tmp AS src
            ON tgt.order_item_id = src.order_item_id
            WHEN MATCHED THEN
                UPDATE
                SET product_id     = src.product_id,
                    order_id       = src.order_id,
                    order_item_num = src.order_item_num,
                    price          = src.price,
                    freight_value  = src.freight_value,
                    load_dttm      = src.load_dttm,
                    load_src       = src.load_src
            WHEN NOT MATCHED THEN
                INSERT (order_item_id, product_id, order_id, order_item_num, price, freight_value, load_dttm, load_src)
                VALUES (src.order_item_id, src.product_id, src.order_id, src.order_item_num, src.price, src.freight_value, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS l_order_items_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_customers_geolocation(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "sat_customers_geolocation"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_customers_geolocation_tmp;

            CREATE LOCAL TEMP TABLE sat_customers_geolocation_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hc.id               AS join_key,
                hc.id               AS customer_id,
                c.zip_code,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.customers c
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers hc ON c.customer_id = hc.customer_id
            UNION ALL
            SELECT
                NULL                AS join_key,
                hc.id               AS customer_id,
                c.zip_code,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.customers c
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers hc ON c.customer_id = hc.customer_id
                    JOIN LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_geolocation scg
                        ON hc.id = scg.customer_id
            WHERE 1 = 1
            AND (c.zip_code <> scg.zip_code AND scg.valid_to IS NULL)
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_geolocation AS tgt
            USING sat_customers_geolocation_tmp AS src
            ON src.join_key = tgt.customer_id
            WHEN MATCHED AND src.zip_code <> tgt.zip_code
                THEN
                UPDATE
                SET
                    valid_to = getutcdate()::timestamp(0)
            WHEN NOT MATCHED THEN
                INSERT
                    (customer_id, zip_code, valid_from, valid_to, load_dttm, load_src)
                VALUES
                    (src.customer_id,
                    src.zip_code,
                    getutcdate()::timestamp(0),
                    NULL, src.load_dttm, src.load_src);


            DROP TABLE IF EXISTS sat_customers_geolocation_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_customers_info(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "sat_customers_info"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_customers_info_tmp;

            CREATE LOCAL TEMP TABLE sat_customers_info_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hc.id AS customer_id,
                c.customer_unique_id,
                c.customer_name,
                c.date_of_birth,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.customers c
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers hc ON c.customer_id = hc.customer_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_info AS tgt
            USING sat_customers_info_tmp AS src
            ON src.customer_id = tgt.customer_id
            WHEN MATCHED
                THEN
                UPDATE
                SET
                    customer_unique_id = src.customer_unique_id,
                    customer_name      = src.customer_name,
                    date_of_birth      = src.date_of_birth,
                    load_dttm          = src.load_dttm,
                    load_src           = src.load_src
            WHEN NOT MATCHED THEN
                INSERT
                (customer_id, customer_unique_id, customer_name, date_of_birth, load_dttm, load_src)
                VALUES
                    (src.customer_id, src.customer_unique_id, src.customer_name, src.date_of_birth, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS sat_customers_info_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_customers_contacts(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "sat_customers_contacts"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_customers_contacts_tmp;

            CREATE LOCAL TEMP TABLE sat_customers_contacts_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hc.id AS customer_id,
                c.email,
                c.phone_number,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.customers c
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_customers hc ON c.customer_id = hc.customer_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_customers_contacts AS tgt
            USING sat_customers_contacts_tmp AS src
            ON src.customer_id = tgt.customer_id
            WHEN MATCHED
                THEN
                UPDATE
                SET
                    email        = src.email,
                    phone_number = src.phone_number,
                    load_dttm    = src.load_dttm,
                    load_src     = src.load_src
            WHEN NOT MATCHED THEN
                INSERT
                    (customer_id, email, phone_number, load_dttm, load_src)
                VALUES
                    (src.customer_id, src.email, src.phone_number, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS sat_customers_contacts_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_reviews(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "sat_reviews"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_reviews_tmp;

            CREATE LOCAL TEMP TABLE sat_reviews_tmp ON COMMIT PRESERVE ROWS AS
            SELECT
                hr.id AS review_id,
                r.review_score,
                r.comment_title,
                r.comment_message,
                r.creation_date,
                r.answer_timestamp,
                r.is_commented,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.reviews r
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_reviews hr ON r.review_id = hr.review_id
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_reviews AS tgt
            USING sat_reviews_tmp AS src
            ON src.review_id = tgt.review_id
            WHEN MATCHED
                THEN
                UPDATE
                SET
                    review_score     = src.review_score,
                    comment_title    = src.comment_title,
                    comment_message  = src.comment_message,
                    creation_date    = src.creation_date,
                    answer_timestamp = src.answer_timestamp,
                    is_commented     = src.is_commented,
                    load_dttm        = src.load_dttm,
                    load_src         = src.load_src
            WHEN NOT MATCHED THEN
                INSERT
                    (review_id, review_score, comment_title, comment_message, creation_date, answer_timestamp, is_commented, load_dttm, load_src)
                VALUES
                    (src.review_id, src.review_score, src.comment_title, src.comment_message, src.creation_date, src.answer_timestamp, src.is_commented, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS sat_reviews_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_category(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "sat_category"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_category_tmp;

            CREATE LOCAL TEMP TABLE sat_category_tmp ON COMMIT PRESERVE ROWS AS
            SELECT DISTINCT
                hc.id               AS category_id,
                p.category_name_eng AS category_name,
                p.category_name     AS category_name_origin,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.products p
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_categories hc ON hash(p.category_name_eng) = hc.id
            WHERE 1 = 1
            AND p.category_name_eng IS NOT NULL
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_category AS tgt
            USING sat_category_tmp AS src
            ON src.category_id = tgt.category_id
            WHEN MATCHED
                THEN
                UPDATE
                SET
                    category_name        = src.category_name,
                    category_name_origin = src.category_name_origin,
                    load_dttm            = src.load_dttm,
                    load_src             = src.load_src
            WHEN NOT MATCHED THEN
                INSERT
                    (category_id, category_name, category_name_origin, load_dttm, load_src)
                VALUES
                    (src.category_id, src.category_name, src.category_name_origin, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS sat_category_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_products_names(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "sat_products_names"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_products_names_tmp;

            CREATE LOCAL TEMP TABLE sat_products_names_tmp ON COMMIT PRESERVE ROWS AS
            SELECT DISTINCT
                hp.id               AS product_id,
                p.product_name,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.products p
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_products hp ON p.product_id = hp.product_id
            WHERE 1 = 1
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_products_names AS tgt
            USING sat_products_names_tmp AS src
            ON src.product_id = tgt.product_id
            WHEN MATCHED
                THEN
                UPDATE
                SET
                    product_name = src.product_name,
                    load_dttm    = src.load_dttm,
                    load_src     = src.load_src
            WHEN NOT MATCHED THEN
                INSERT
                    (product_id, product_name, load_dttm, load_src)
                VALUES
            (src.product_id, src.product_name, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS sat_products_names_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_products_dimensions(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "update_sat_products_dimensions"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_products_dimensions_tmp;

            CREATE LOCAL TEMP TABLE sat_products_dimensions_tmp ON COMMIT PRESERVE ROWS AS
            SELECT DISTINCT
                hp.id               AS product_id,
                p.weight_g,
                p.length_cm,
                p.height_cm,
                p.width_cm,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.products p
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_products hp ON p.product_id = hp.product_id
            WHERE 1 = 1
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_products_dimensions AS tgt
            USING sat_products_dimensions_tmp AS src
            ON src.product_id = tgt.product_id
            WHEN MATCHED
                THEN
                UPDATE
                SET
                    weight_g  = src.weight_g,
                    length_cm = src.length_cm,
                    height_cm = src.height_cm,
                    width_cm  = src.width_cm,
                    load_dttm = src.load_dttm,
                    load_src  = src.load_src
            WHEN NOT MATCHED THEN
                INSERT
                    (product_id, weight_g, length_cm, height_cm, width_cm, load_dttm, load_src)
                VALUES
                    (src.product_id, src.weight_g, src.length_cm, src.height_cm, src.width_cm, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS sat_products_dimensions_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError

    def update_sat_sellers_info(self):

        LOAD_DTTM = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        LOAD_SOURCE = "s3:data-ice-lake-04"
        TABLE = "sat_sellers_info"

        logger.info(f"Updating `{TABLE}` table.")

        SQL = f"""
            DROP TABLE IF EXISTS sat_sellers_info_tmp;

            CREATE LOCAL TEMP TABLE sat_sellers_info_tmp ON COMMIT PRESERVE ROWS AS
            SELECT DISTINCT
                hs.id               AS seller_id,
                s.email,
                s.phone_number,
                '{LOAD_DTTM}'::timestamp(0) AS load_dttm,
                '{LOAD_SOURCE}'             AS load_src
            FROM LEONIDGRISHENKOVYANDEXRU__STAGING.sellers s
                    LEFT JOIN LEONIDGRISHENKOVYANDEXRU__DWH.hub_sellers hs ON s.seller_id = hs.seller_id
            WHERE 1 = 1
                UNSEGMENTED ALL NODES;

            MERGE INTO LEONIDGRISHENKOVYANDEXRU__DWH.sat_sellers_info AS tgt
            USING sat_sellers_info_tmp AS src
            ON src.seller_id = tgt.seller_id
            WHEN MATCHED
                THEN
                UPDATE
                SET
                    email        = src.email,
                    phone_number = src.phone_number,
                    load_dttm    = src.load_dttm,
                    load_src     = src.load_src
            WHEN NOT MATCHED THEN
                INSERT
                    (seller_id, email, phone_number, load_dttm, load_src)
                VALUES
                    (src.seller_id, src.email, src.phone_number, src.load_dttm, src.load_src);

            DROP TABLE IF EXISTS sat_sellers_info_tmp;
        """

        try:
            cur = self.dwh_conn.cursor()
            logger.info("Executing update statement...")
            cur.execute(operation=SQL)
            logger.info(f"`{TABLE}` was successfully updated.")
        except Exception:
            logger.exception(
                f"Unable to execute update statement. Updating process for `{TABLE}` failed!"
            )
            raise VerticaError


def do_testing() -> None:

    logger.info("Lets do some testing. 🤓")

    # creator = DWHCreator()
    # creator.create_stg_layer()

    # getter = DataGetter()
    # getter.prepare_customers_data()
    # getter.prepare_geolocations_data()
    # getter.prepare_orders_data()
    # getter.prepare_products_data()
    # getter.prepare_sellers_data()
    # getter.prepare_order_items_data()
    # getter.prepare_order_payments_data()
    # getter.prepare_order_reviews_data()

    # getter.get_all_prepared_data()

    # data_loader = STGDataLoader()
    # data_loader.load_products_data_to_dwh()
    # data_loader.load_customers_data_to_dwh()
    # data_loader.load_geolocation_data_to_dwh()
    # data_loader.load_sellers_data_to_dwh()
    # data_loader.load_reviews_data_to_dwh()
    # data_loader.load_payments_data_to_dwh()
    # data_loader.load_order_items_data_to_dwh()
    # data_loader.load_orders_data_to_dwh()

    data_loader = DDSDataLoader()
    # data_loader.update_hub_customers()
    # data_loader.update_hub_orders()
    # data_loader.update_hub_products()
    # data_loader.update_hub_categories()
    # data_loader.update_hub_sellers()
    # data_loader.update_hub_reviews()
    # data_loader.update_hub_geolocations()
    # data_loader.update_hub_cities()
    # data_loader.update_l_order_customer()
    # data_loader.update_l_customer_geolocation()
    # data_loader.update_l_geolocation_city()
    # data_loader.update_l_seller_geolocation()
    # data_loader.update_l_product_seller()
    # data_loader.update_l_product_category()
    # data_loader.update_l_review_order()
    # data_loader.update_l_order_items()

    # data_loader.update_sat_customers_geolocation()
    # data_loader.update_sat_customers_info()
    # data_loader.update_sat_customers_contacts()
    # data_loader.update_sat_reviews()
    # data_loader.update_sat_category()
    # data_loader.update_sat_products_names()
    # data_loader.update_sat_products_dimensions()
    data_loader.update_sat_sellers_info()


if __name__ == "__main__":
    do_testing()
