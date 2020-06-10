# Import Airflow dependencies and libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Define default arguments such that the tasks are run daily and are
# dependent on each other
default_args = {
    'owner': 'Francis',
    'depends_on_past': True,
#    'start_date': datetime(2020, 6, 6),
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
	dag_id = 'Daily311_DAG', 
	description = 'Performs Batch Processing on Daily 311 Calls', 
	default_args = default_args, 
	schedule_interval = '@daily')

# Retrieving new 311 calls via Socrata API
retrieve_data = BashOperator(
  task_id='retrieve311Data',
  bash_command='python3 /home/ubuntu/airflow/tasks/retrieveDaily311.py',
  dag = dag)

# Spark Job 
batch_process_data = BashOperator(
  task_id='batch_process',
  bash_command="spark-submit --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 --master spark://<MASTER NODE DNS>:7077 --driver-class-path /home/ubuntu/airflow/tasks/postgresql-42.2.12.jar --jars /home/ubuntu/airflow/tasks/postgresql-42.2.12.jar /home/ubuntu/airflow/tasks/batch_process_daily.py --py-files /home/ubuntu/airflow/tasks/batch_process_functions.py",
  dag = dag)

# Set retrieve_data to be run first followed by batch_process_data
retrieve_data >> batch_process_data

