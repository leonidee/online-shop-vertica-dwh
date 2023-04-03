import sys
import boto3
from pathlib import Path
import logging

# environment
from dotenv import load_dotenv, find_dotenv
from os import getenv

# typing
from enum import Enum
from vertica_python import Connection
from boto3.session import Session

# sql
import vertica_python

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))
from package.errors import DotEnvError, S3ServiceError, VerticaError

logger = logging.getLogger("airflow.task")


class Connector:
    def __init__(self, type: Enum("dwh", "ice-lake")) -> None:
        self.type = type

    def _get_credentials(self) -> dict:
        logger.info("Getting environment variables from .env file.")
        try:
            find_dotenv(raise_error_if_not_found=True)
            logger.info(".env was found.")
        except Exception:
            logger.exception("Can't find .env file!")
            raise DotEnvError

        try:
            load_dotenv(verbose=True, override=True)
            logger.info(".env loaded.")
        except Exception:
            logger.exception("Unable to read .env! Please check if its accessible.")
            raise DotEnvError

        if self.type == "dwh":
            try:
                holder = dict(
                    host=getenv("VERTICA_DWH_HOST"),
                    port=getenv("VERTICA_DWH_PORT"),
                    user=getenv("VERTICA_DWH_USER"),
                    password=getenv("VERTICA_DWH_PASSWORD"),
                    database=getenv("VERTICA_DWH_DB"),
                    autocommit=False,
                    log_level=logging.INFO,
                    log_path="",
                )
                logger.info(f"All variables loaded for `{self.type}` connection.")
            except Exception:
                logger.exception(
                    f"Unable to get one of or all variables for `{self.type}` connection!"
                )
                raise DotEnvError

        if self.type == "ice-lake":
            try:
                holder = dict(
                    key=getenv("S3_ACCESS_KEY"),
                    secret=getenv("S3_SECRET_KEY"),
                    endpoint="https://storage.yandexcloud.net",
                )
                logger.info(f"All variables loaded for `{self.type}` connection.")
            except Exception:
                logger.exception(
                    f"Unable to get one of or all variables for `{self.type}` connection!"
                )
                raise DotEnvError

        return holder

    def connect(self) -> Connection | Session:

        holder = self._get_credentials()

        if self.type == "dwh":
            logger.info(f"Connecting to vertica `{holder['database']}` database.")

            try:
                conn = vertica_python.connect(**holder)
                logger.info(
                    f"Succesfully connected to vertica `{holder['database']}` database."
                )
            except Exception:
                logger.exception(
                    f"Connection to vertica `{holder['database']}` database failed!"
                )
                raise VerticaError

        if self.type == "ice-lake":
            logger.info(f"Connecting to s3 service.")

            try:
                session = boto3.session.Session()

                conn = session.client(
                    service_name="s3",
                    region_name="ru-central1",
                    endpoint_url=holder["endpoint"],
                    aws_access_key_id=holder["key"],
                    aws_secret_access_key=holder["secret"],
                )
                logger.info(f"Succesfully connected to s3 service.")
            except Exception:
                logger.exception("Connection to s3 service failed!")
                raise S3ServiceError

        return conn
