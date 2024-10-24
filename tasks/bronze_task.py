from bronze.API_parquet_creation import parquet_creation
from utils.config import api_key, companies
from datetime import timedelta


def bronze_task_f(**context):

    execution_date = context['execution_date'] - timedelta(days=1)

    execution_date_str = execution_date.strftime('%Y-%m-%d')

    parquet_creation(companies, api_key, execution_date_str)


if __name__ == "__main__":
    bronze_task_f()
