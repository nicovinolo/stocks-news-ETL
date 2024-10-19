import os
import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Adjust path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # noqa: E402

# Import tasks
from tasks.bronze_task import bronze_task_f  # noqa: E402
from tasks.silver_task_02 import silver_task_02_f  # noqa: E402
from tasks.silver_task_01 import silver_task_01_f  # noqa: E402
from tasks.gold_task import gold_task_f  # noqa: E402

default_args = {
    "owner": "nicolas_vinolo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}

with DAG(
    dag_id="stock-news-etl",
    default_args=default_args,
    description="ETL to extract news company data from an API",
    schedule_interval='0 16 * * *',
    start_date=days_ago(3),
    catchup=True,
    max_active_runs=1,
) as dag:

    bronze_task_DAG = PythonOperator(
        task_id="bronze_task",
        python_callable=bronze_task_f
    )

    silver_task_01_DAG = PythonOperator(
        task_id="silver_task_01",
        python_callable=silver_task_01_f
    )

    silver_task_02_DAG = PythonOperator(
        task_id="silver_task_02",
        python_callable=silver_task_02_f
    )

    gold_task_DAG = PythonOperator(
        task_id="gold_task",
        python_callable=gold_task_f
    )

    # Define task execution sequence
    bronze_task_DAG >> silver_task_01_DAG >> silver_task_02_DAG >> gold_task_DAG
