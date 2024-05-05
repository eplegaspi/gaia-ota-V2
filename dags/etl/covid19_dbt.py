from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG('covid19_dbt', 
         start_date=datetime(2021, 1, 1),
         catchup=False,
         schedule_interval='@daily'
        ) as dag:
    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command='dbt run --project-dir=/opt/airflow/dags/covid19 --profiles-dir=/opt/airflow/dags/covid19/profiles',
        dag=dag
    )