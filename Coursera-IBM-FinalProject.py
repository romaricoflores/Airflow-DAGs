# Import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


# Defining DAG Arguments
default_args = {
    'owner': 'Roma Rico Flores',
    'start_date': days_ago(0),
    'email': ['floresromarico@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1),
)

# Task 1 - Unzip Data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -zxf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

#Task 2 - Extract Data from CSV
extract_data_from_csv = BashOperator(
    task_id = 'extractr_data_from_csv',
    bash_command = 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# Task 3 - Extract Data from TSV
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 tollplaza-data.csv > tsv_data.csv',
    dag=dag,
)

# Task 4 - Extract Data from Fixed Width
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = "cat payment-data.txt | tr -s '[:space:]' | cut -d' ' -f11,12 > fixed_width_data.csv",
    dag=dag, 
)

# Consolidate Data
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste csv_data.csv tsv_data.tsv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# Transform Data
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'tr "[a-z]" "[A-Z]" < extracted_data.csv > transformed-data.csv',
    dag=dag,
)

# Task Pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

