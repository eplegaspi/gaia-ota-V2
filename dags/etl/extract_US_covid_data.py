# default
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


# 3rd party imports
import pandas as pd
import minio.error


# local imports
from services.minio_service import connect_to_minio, fetch_file_from_url, store_file_in_minio, download_parquet_from_minio
from services.postgresql_service import connect_to_postgresql, insert_us_data_into_postgresql


with DAG('extract_US_covid_data', 
         start_date=datetime(2021, 1, 1),
         end_date=datetime(2024, 1, 1),
         catchup=True,
         schedule_interval='@daily',
        # # this will allow up to 4 dags to be run at the same time
        max_active_runs=16
        ) as dag:


    def filter_and_upload_to_minio(**kwargs):
        ds = kwargs['ds']
        date = datetime.strptime(ds, '%Y-%m-%d').strftime('%m-%d-%Y')
        year = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
        month = datetime.strptime(ds, '%Y-%m-%d').strftime('%m')
        api_url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/{date}.csv"
        print(api_url)

        minio_client = connect_to_minio()

        file_stream = fetch_file_from_url(api_url)

        if file_stream:
            store_file_in_minio(
                minio_client,
                "coviddata",
                f"US/{year}/{month}/{date}.csv",
                file_stream,
                file_stream.getbuffer().nbytes
            )
        else:
            print("Skipping.")


    def download_and_insert_to_postgresql(**kwargs):
        ds = kwargs['ds']
        date = datetime.strptime(ds, '%Y-%m-%d').strftime('%m-%d-%Y')
        year = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')
        month = datetime.strptime(ds, '%Y-%m-%d').strftime('%m')

        minio_client = connect_to_minio()

        try:
            download_parquet_from_minio(
                minio_client,
                "coviddata",
                f"US/{year}/{month}/{date}.csv",
                f"{year}/{month}/{date}.csv"
            )

            df = pd.read_csv(f"{year}/{month}/{date}.csv", header=0)

            connection = connect_to_postgresql()

            if connection:
                batch_size = 100000
                insert_us_data_into_postgresql(connection, df, batch_size)
                connection.close()
        except minio.error.NoSuchKey:
            print("File not found.")
        except Exception as e:
            print(f"An error occurred: {e}")


    filter_and_upload_task = PythonOperator(
        task_id='filter_and_upload_to_minio_task',
        python_callable=filter_and_upload_to_minio,
        provide_context=True
    )
    

    download_and_insert_task = PythonOperator(
        task_id='download_and_insert_to_postgresql_task',
        python_callable=download_and_insert_to_postgresql,
        provide_context=True
    )


    filter_and_upload_task >> download_and_insert_task