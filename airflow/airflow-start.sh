#!/bin/bash
airflow standalone & 

# Wait for Airflow webserver to become available
while true; do
    if curl -sSf "http://localhost:8080/health" &> /dev/null; then
        break
    else
        echo "Waiting for Airflow webserver to start..."
        sleep 5
    fi
done

# Perform database migration
airflow db migrate

# Create Airflow user
airflow users create \
    --username airflow \
    --firstname airflow \
    --lastname airflow \
    --role Admin \
    --email airflow@test.org \
    --password airflow

if [ $? -eq 0 ]; then
    echo "User airflow creation successful"
else
    echo "User airflow creation failed"
fi

# Start Airflow scheduler
airflow scheduler

# Start Airflow webserver
airflow webserver --port 8080