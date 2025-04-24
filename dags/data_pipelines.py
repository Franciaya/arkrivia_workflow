from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka_process.producer_jsonbin import send_patient_data
from kafka_process.consumer import main 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_spark_delta_pipeline',
    default_args=default_args,
    description='Ingest from API using Kafka producer and stream to Delta via Spark',
    schedule_interval='@once',
    catchup=False
)

# Task 1: Ingest data from API using Kafka producer
produce_data = PythonOperator(
    task_id='run_kafka_producer',
    python_callable=send_patient_data,
    dag=dag,
)


# Task 2: Consume, transform and load data to Delta Lake
consume_data = PythonOperator(
    task_id='run_kafka_spark_consumer_transformer',
    python_callable=main,
    dag=dag,
)


produce_data >> consume_data