from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka_process.producer_jsonbin import send_patient_data
from kafka_process.consumer import process


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "kafka_spark_delta_pipeline",
    default_args=default_args,
    description="Ingest from API using Kafka producer and stream to Delta via Spark",
    schedule_interval="@daily",
    catchup=False,
)

# Task 1: Ingest data from API using Kafka producer
produce_data = PythonOperator(
    task_id="run_kafka_producer",
    python_callable=send_patient_data,
    dag=dag,
    on_failure_callback=lambda context: print(f"Producer task failed: {context}"),
)


# Task 2: Consume, transform and load data to Delta Lake
consume_data = PythonOperator(
    task_id="run_kafka_spark_consumer_transformer",
    python_callable=process,
    dag=dag,
    on_failure_callback=lambda context: print(f"Producer task failed: {context}"),
)


produce_data
consume_data
