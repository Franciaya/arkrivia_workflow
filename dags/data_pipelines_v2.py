from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_spark_delta_pipeline_v2',
    default_args=default_args,
    description='Ingest from API using Kafka producer and stream to Delta via Spark',
    schedule_interval='@daily',
    catchup=False,
)

# Read script paths from Airflow Variables
producer_script = Variable.get('producer_script_dir')
consumer_script = Variable.get('consumer_script_dir')

# Task 1: Run Kafka Producer script using BashOperator
# produce_data = BashOperator(
#     task_id='run_kafka_producer',
#     bash_command=f'python {producer_script}',
#     dag=dag,
# )

# Task 2: Run Kafka Consumer script using BashOperator
consume_data = BashOperator(
    task_id='spark_consumer_transformer',
    bash_command=f'python {consumer_script}',
    dag=dag,
)

# Define task dependencies
# produce_data
consume_data
