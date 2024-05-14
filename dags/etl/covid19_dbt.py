from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG('covid19_dbt', 
         start_date=datetime(2021, 1, 1),
         catchup=False,
         schedule_interval='@daily'
        ) as dag:
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir=/opt/airflow/dags/covid19 --profiles-dir=/opt/airflow/dags/covid19/profiles',
        dag=dag
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir=/opt/airflow/dags/covid19 --profiles-dir=/opt/airflow/dags/covid19/profiles',
        dag=dag
    )

    dbt_run >> dbt_test