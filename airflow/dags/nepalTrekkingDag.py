from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Define the path to your Python script
SCRIPT_PATH = os.path.join(os.path.dirname(__file__), 'nepal_trekking_routes_scraper.py')

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'nepal_trekking_routes_scraper',
    default_args=default_args,
    description='A DAG to scrape content from Nepal Trekking Routes website',
    schedule_interval='0 0 * * 0',  # Run every Sunday at midnight
    catchup=False,
)

# Define the task that runs the Python script
def run_scraper():
    import subprocess
    subprocess.run(['python3', SCRIPT_PATH], check=True)

run_scraper_task = PythonOperator(
    task_id='run_scraper',
    python_callable=run_scraper,
    dag=dag,
)

# Set the task in the DAG
run_scraper_task