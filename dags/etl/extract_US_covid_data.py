# default
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

# 3rd party imports
import pandas as pd
import minio.error
from dotenv import load_dotenv

# local imports
from services.minio_service import MinioConnector
from services.postgresql_service import PostgresConnector

load_dotenv()

minio_endpoint = os.getenv('MINIO_ENDPOINT')
minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')

postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_host = os.getenv('POSTGRES_HOST')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_db = os.getenv('POSTGRES_DB')

with DAG('extract_US_covid_data', 
         start_date=datetime(2021, 1, 1),
         end_date=datetime(2024, 1, 1),
         catchup=True,
         schedule_interval='@daily',
         max_active_runs=16
        ) as dag:


    def api_to_minio(**kwargs):
        ds = kwargs['ds']
        date = datetime.strptime(ds, '%Y-%m-%d').strftime('%m-%d-%Y')
        year = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
        month = datetime.strptime(ds, '%Y-%m-%d').strftime('%m')
        api_url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/{date}.csv"
        print(api_url)

        minio_conn = MinioConnector(minio_endpoint, minio_access_key, minio_secret_key)

        file_stream = minio_conn.fetch_file_from_url(api_url)

        if file_stream:
            minio_conn.store_file(
                "coviddata",
                f"US/{year}/{month}/{date}.csv",
                file_stream,
                file_stream.getbuffer().nbytes
            )
        else:
            print("Skipping.")


    def minio_to_warehouse(**kwargs):
        ds = kwargs['ds']
        date = datetime.strptime(ds, '%Y-%m-%d').strftime('%m-%d-%Y')
        year = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
        month = datetime.strptime(ds, '%Y-%m-%d').strftime('%m')
        file_date = datetime.strptime(ds, '%Y-%m-%d')

        minio_conn = MinioConnector(minio_endpoint, minio_access_key, minio_secret_key)

        try:
            minio_conn.download_file(
                "coviddata",
                f"US/{year}/{month}/{date}.csv",
                f"{year}/{month}/{date}.csv"
            )

            df = pd.read_csv(f"{year}/{month}/{date}.csv", header=0)

            postgresql_conn = PostgresConnector(
                user=postgres_user,
                password=postgres_password,
                host=postgres_host,
                port=postgres_port,
                database=postgres_db
            )

            batch_size = 100000
            postgresql_conn.insert_us_data(postgresql_conn, df, batch_size, file_date)
            postgresql_conn.close_connection()
        except minio.error.S3Error:
            print("File not found.")
        except Exception as e:
            print(f"An error occurred: {e}")


    api_to_minio_task = PythonOperator(
        task_id='api_to_minio',
        python_callable=api_to_minio,
        provide_context=True
    )
    

    minio_to_warehouse_task = PythonOperator(
        task_id='minio_to_warehouse',
        python_callable=minio_to_warehouse,
        provide_context=True
    )


    api_to_minio_task >> minio_to_warehouse_task