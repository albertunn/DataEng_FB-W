from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from kaggle_downloader import download_kaggle_dataset
from clickhouse_loader import load_to_clickhouse
from iceberg_writer import create_iceberg_table_from_csv

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
    create_iceberg = PythonOperator(
        task_id="write_iceberg_table",
        python_callable=create_iceberg_table_from_csv,
        op_kwargs={"data_dir": "/opt/airflow/data/espn_soccer"},
    )
    load_task = PythonOperator(
    task_id="load_to_clickhouse_star_schema",
    python_callable=load_to_clickhouse,
    op_kwargs={
            "data_dir": "/opt/airflow/data/espn_soccer",
            "run_date": "{{ ds }}"
        },
    )
    trigger_weather_dag = TriggerDagRunOperator(
    task_id='trigger_weather_fetch',
    trigger_dag_id='fetch_match_weather',
    wait_for_completion=False,
)

download_task >> create_iceberg >> load_task >> trigger_weather_dag