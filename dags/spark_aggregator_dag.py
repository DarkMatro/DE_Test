"""
DAG for running daily CRUD log aggregation.

This DAG is scheduled to run daily at 7:00 AM, executing a bash command to run a Python script
that processes CRUD logs and aggregates them over the past 7 days. The task uses
Polars for data processing and stores intermediate results to avoid reprocessing.

DAG schedule: 7:00 AM daily

Input: Daily log files in CSV format
Output: Aggregated results stored in CSV format
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Определяем директории на хосте, которые будут замаунчены в контейнеры (МОЖЕТ БЫТЬ ИЗМЕНЕНО)
INPUT_DIR = "./input"
OUTPUT_DIR = "./output"
CACHE_DIR = "./cache"

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_aggregator_dag',
    default_args=default_args,
    description='A DAG to run Spark aggregation job daily at 7:00 AM',
    schedule_interval='0 7 * * *',  # Запуск ежедневно в 7:00 утра
    catchup=False,
) as dag:
    # Операция для выполнения Spark задачи
    run_spark_job = BashOperator(
        task_id='run_spark_job',
        bash_command=f"spark-submit --master local /dags/spark_job.py {INPUT_DIR} {OUTPUT_DIR} {{ "
                     f"ds }}",
        dag=dag,
    )
