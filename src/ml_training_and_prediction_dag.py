from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import os

PROJECT_ID = "ai-health-risk-system"
REGION = "us-central1"   # match your BigQuery dataset region
SQL_PATH = "/home/airflow/gcs/dags/sql_scripts"

default_args = {
    "owner": "ml-engineer",
    "email_on_failure": True,
    "email": ["tumatipraveenreddy18@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

def load_sql(filename):
    path = os.path.join(SQL_PATH, filename)
    with open(path, "r") as f:
        return f.read()

with DAG(
    dag_id="ml_training_and_prediction_dag",
    default_args=default_args,
    description="Test ML training + prediction (every 20 min)",
    schedule_interval="*/20 * * * *",  # every 20 minutes for testing
    start_date=datetime(2025, 10, 30),
    catchup=False,
    max_active_runs=1,
    tags=["bqml", "ml", "prediction", "test"],
) as dag:

    model_training = BigQueryInsertJobOperator(
        task_id="model_training",
        configuration={
            "query": {"query": load_sql("model_training.sql"), "useLegacySql": False}
        },
        location=REGION,
    )

    model_prediction = BigQueryInsertJobOperator(
        task_id="model_prediction",
        configuration={
            "query": {"query": load_sql("predict.sql"), "useLegacySql": False}
        },
        location=REGION,
    )

    model_training >> model_prediction
