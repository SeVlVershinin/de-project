#!/usr/bin/env python3
import psycopg2

from py_scripts.settings import Settings

from py_scripts.etl_tasks import load_data_into_dwh
from py_scripts.report_generators import generate_reports

settings = Settings()


def get_db_connection():
    return psycopg2.connect(
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
        host=settings.host,
        port=settings.port
    )


def main():
    for current_date in settings.processing_dates:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                load_data_into_dwh(current_date, cursor)
                connection.commit()

                generate_reports(current_date, cursor)
                connection.commit()

main()
