from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 6, 2),
    'email': ['floresromarico@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='sample_project',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False
)

def extract_data():
    df = pd.read_csv('/path/to/your/data.csv')   # Replace with the actual path to your CSV file
    # Do further processing or transformations on the DataFrame if needed
    print(df.head())  # Example: print the first few rows of the DataFrame

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)