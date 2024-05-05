FROM apache/airflow:2.8.3

COPY ./airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root

COPY ./airflow/airflow-start.sh /airflow-start.sh
RUN chmod +x /airflow-start.sh
USER airflow

ENTRYPOINT ["/bin/bash","/airflow-start.sh"]