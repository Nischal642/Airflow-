from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use correct container path
SOURCE_JSON_PATH = "/opt/airflow/Output/Nepal_Profile.json"
POSTGRES_CONN_ID = "postgres_default"
SCHEMA_NAME = "tourism"
TABLE_NAME = "nepal_profile"
BATCH_SIZE = 500  # Insert 500 records at a time

def sanitize_text(text):
    """Remove NUL (0x00) characters from text fields."""
    return text.replace("\x00", "") if text else text

def insert_json_to_postgres():
    try:
        # Verify file exists
        logger.info(f"Checking file: {SOURCE_JSON_PATH}")
        if not os.path.exists(SOURCE_JSON_PATH):
            raise FileNotFoundError(f"File not found: {SOURCE_JSON_PATH}")

        # Connect to Postgres
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Read JSON file
        with open(SOURCE_JSON_PATH, "r") as f:
            data = json.load(f)["data"]

        total_records = len(data)
        logger.info(f"Total records to insert: {total_records}")

        # Batch insert with NUL character removal
        for i in range(0, total_records, BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            records_to_insert = [
                (sanitize_text(record["title"]), sanitize_text(record["contents"]), json.dumps(record["image_ids"]))
                for record in batch
            ]
            cursor.executemany(
                f"INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (title, contents, image_ids) VALUES (%s, %s, %s)",
                records_to_insert
            )
            conn.commit()
            logger.info(f"Inserted batch {i // BATCH_SIZE + 1} ({len(records_to_insert)} records)")

        # Close connection
        cursor.close()
        conn.close()
        logger.info("Successfully inserted all data into PostgreSQL!")

    except Exception as e:
        logger.error(f"Failed: {str(e)}")
        raise

# DAG configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 20),
    
}

dag = DAG(
    "store_json_to_postgres",
    default_args=default_args,
    schedule_interval=None,  # Manually trigger
    catchup=False,
)

# Define Airflow Task
store_json_task = PythonOperator(
    task_id="insert_json_to_db",
    python_callable=insert_json_to_postgres,
    dag=dag,
)

# Set task order
store_json_task
