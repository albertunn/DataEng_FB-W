from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kaggle_downloader import download_kaggle_dataset
from clickhouse_loader import load_to_clickhouse

with DAG(
    dag_id="update_kaggle_espn_soccer",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    download_task = PythonOperator(
        task_id="download_kaggle_data",
        python_callable=download_kaggle_dataset,
    )
    load_task = PythonOperator(
    task_id="load_to_clickhouse_star_schema",
    python_callable=load_to_clickhouse,
    op_kwargs={
            "data_dir": "/opt/airflow/data/espn_soccer",
            "run_date": "{{ ds }}"
        },
)

download_task >> load_task