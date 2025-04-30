import os
import sys
import logging
logging.getLogger().warning("airflow_local_settings.py has been loaded!")
logging.getLogger().warning("sys.path = %s", sys.path)


# Get the Airflow home directory from environment variable
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

if not AIRFLOW_HOME:
    raise EnvironmentError("AIRFLOW_HOME environment variable is not set!")

# Set the PROJECT_PATH dynamically based on AIRFLOW_HOME
PROJECT_PATH = AIRFLOW_HOME  

# Add custom project directories to sys.path
sys.path.insert(0, PROJECT_PATH) 

# Add kafka_process and spark_process to sys.path so Airflow can find them as packages
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'kafka_process'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'spark_process'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'config'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'data'))
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'ingestion_jsonio_api'))

# Print paths for debugging (optional)
print(f"PROJECT_PATH set to: {PROJECT_PATH}")
print(f"Current sys.path: {sys.path}")