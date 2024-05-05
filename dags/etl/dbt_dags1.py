from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG('dag1', 
         start_date=datetime(2021, 1, 1),
         catchup=False,
         schedule_interval='@daily'
        # # this will allow up to 4 dags to be run at the same time
        # max_active_runs=1
        ) as dag:
    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command='dbt run --project-dir=/opt/airflow/dags/covid19 --profiles-dir=/opt/airflow/dags/covid19/profiles',
        dag=dag
    )