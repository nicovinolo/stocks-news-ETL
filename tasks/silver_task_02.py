from datetime import timedelta

from utils.engine_creation import create_redshift_engine_from_env

from silver.db_transformation import table_tansformation


def silver_task_02_f(**context):

    try:

        connection = create_redshift_engine_from_env()

        execution_date = context['execution_date'] - timedelta(days=1)

        execution_date_str = execution_date.strftime('%Y-%m-%d')

        table_tansformation(connection, execution_date_str)


if __name__ == "__main__":
    silver_task_02_f()
