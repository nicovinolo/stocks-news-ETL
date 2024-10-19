from airflow.exceptions import AirflowException
from datetime import timedelta

from utils.engine_creation import create_redshift_engine_from_env

from gold.company_daily_metrics import populate_company_insights


def gold_task_f(**context):

    try:

        connection = create_redshift_engine_from_env()

        execution_date = context['execution_date'] - timedelta(days=1)

        execution_date_str = execution_date.strftime('%Y-%m-%d')

        populate_company_insights(connection, execution_date_str)

    except AirflowException as e:
        raise e


if __name__ == "__main__":
    gold_task_f()
