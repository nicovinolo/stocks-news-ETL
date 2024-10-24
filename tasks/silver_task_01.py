from airflow.exceptions import AirflowException
from datetime import timedelta

from utils.engine_creation import create_redshift_engine_from_env

from silver.db_creation import create_redshift_tables
from silver.db_data_insertion import insert_company_data_into_redshift
from silver.db_data_insertion import insert_news_data_into_redshift


def silver_task_01_f(**context):

    try:
        connection = create_redshift_engine_from_env()

        # Create schema in db if it doesn't exist
        create_redshift_tables(connection)

        execution_date = context['execution_date'] - timedelta(days=1)

        execution_date_str = execution_date.strftime('%Y-%m-%d')

        insert_company_data_into_redshift(connection, execution_date_str)
        insert_news_data_into_redshift(connection, execution_date_str)


if __name__ == "__main__":
    silver_task_01_f()
