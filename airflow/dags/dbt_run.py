from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dbt_run_daily',
    default_args=default_args,
    description='Runs dbt transformations daily after data ingestion',
    schedule_interval='@daily',  # runs once per day
    start_date=datetime(2025, 10, 28),
    catchup=False,
    tags=['dbt', 'transformations'],
) as dag:

    DBT_PROJECT_DIR = '/dbt'

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""docker exec dbt dbt run""",
    )

    dbt_run
