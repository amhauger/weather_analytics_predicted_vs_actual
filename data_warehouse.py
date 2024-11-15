import logging
import os
import psycopg2
import pandas as pd
import datetime

from query_strings import *

log = logging.getLogger(__name__)

year = 2024
october = 10
day = 30


class RedshiftDW:
    host: str
    port: str
    name: str
    user: str
    password: str
    connection: any

    """
    BEGIN HELPERS
    """

    def get_database_configs_from_environment(self) -> None:
        log.info("getting database information from dictionary")
        if os.environ.get("DBHOST", None) == None:
            log.error("no dbhost env")
            raise Exception

        self.host = os.environ.get("DBHOST")
        log.info("database host set in config")

        if os.environ.get("DBPORT", None) == None:
            log.error("no dbport env")
            raise Exception

        self.port = os.environ.get("DBPORT")
        log.info("database port set in config")

        if os.environ.get("DBNAME", None) == None:
            log.error("no dbname env")
            raise Exception

        self.name = os.environ.get("DBNAME")
        log.info("database name set in config")

        if os.environ.get("DBUSER", None) == None:
            log.error("no dbuser env")
            raise Exception

        self.user = os.environ.get("DBUSER")
        log.info("database username set in config")

        if os.environ.get("DBPASSWORD", None) == None:
            log.error("no dbpassword env")
            raise Exception

        self.password = os.environ.get("DBPASSWORD")
        log.info("database password set in config")

    def connect(self) -> None:
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.name,
                user=self.user,
                password=self.password,
            )
        except Exception as e:
            log.error(f"unable to connect to redshift: {e}")
            raise e

        if conn.status != psycopg2.extensions.STATUS_READY:
            log.error("connection not in ready status")
            raise Exception

        self.connection = conn

    def close(self) -> None:
        self.connection.close()
        self.connection = None

    """
    END HELPERS
    """

    """
    BEGIN GETTERS
    """

    def get_store_numbers(self) -> list[str]:
        log.info("getting store numbers from predictions table")
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(get_store_numbers_from_prediction_table)
            return cursor.fetchall()
        except Exception as e:
            log.error(
                f"error getting store numbers from public.weather_analytics_predictions: {e}"
            )
            raise e
        finally:
            cursor.close()
            self.close()

    def get_orders_by_store(self, location_number: str) -> pd.DataFrame:
        log.info(f"getting orders for location: {location_number}")
        starting_time = datetime.datetime(
            year=year,
            month=october,
            day=day,
        ).strftime("%Y-%m-%d")
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                get_orders_by_store_number.format(starting_time, location_number)
            )
            rows = cursor.fetchall()
            return pd.DataFrame(
                rows,
                columns=[desc[0] for desc in cursor.description],
            )
        except Exception as e:
            log.error(f"error getting orders for location {location_number}: {e}")
            raise e
        finally:
            cursor.close()
            self.close()

    def get_most_recent_prediction_for_store_and_datetimes(
        self, location_number: str, prediction_date: str
    ) -> pd.DataFrame:
        log.info(
            f"getting predicted car count for location {location_number} and time {prediction_date}"
        )
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                get_most_recent_prediction_for_store_and_datetime.format(
                    prediction_date, location_number
                )
            )
            rows = cursor.fetchall()
            return pd.DataFrame(
                rows,
                columns=[desc[0] for desc in cursor.description],
            )
        except Exception as e:
            log.error(
                f"error getting predicted car count for location {location_number} at time {prediction_date}: {e}"
            )
            raise e
        finally:
            cursor.close()
            self.close()

    def __init__(self):
        self.get_database_configs_from_environment()
