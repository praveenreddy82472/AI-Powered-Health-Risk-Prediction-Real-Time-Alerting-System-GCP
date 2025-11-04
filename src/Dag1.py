from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import os

# ======== CONFIG ==========
PROJECT_ID = "ai-health-risk-system"
LOCATION = "us-central1"
SQL_PATH = "/home/airflow/gcs/dags/sql_scripts"

default_args = {
    "owner": "data-engineer",
    "email_on_failure": True,
    "email": ["tumatipraveenreddy18@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ======== Helper ==========
def load_sql(filename):
    """Safely load SQL from file."""
    path = os.path.join(SQL_PATH, filename)
    with open(path, "r") as f:
        return f.read()

# ======== DAG ==========
with DAG(
    dag_id="health_sql_transformations",
    default_args=default_args,
    description="Transform streaming data in BigQuery (Raw → Silver → Gold → Features)",
    schedule_interval="*/15 * * * *",  # every 15 minutes
    start_date=datetime(2025, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "etl", "composer"],
) as dag:

    # 1️⃣ Silver layer transformation
    silver_transform = BigQueryInsertJobOperator(
        task_id="silver_transform",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": load_sql("silver_query.sql"),
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    # 2️⃣ Gold layer transformation
    gold_transform = BigQueryInsertJobOperator(
        task_id="gold_transform",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": load_sql("gold_query.sql"),
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    # 3️⃣ Feature engineering (for ML)
    feature_engineering = BigQueryInsertJobOperator(
        task_id="feature_engineering",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": load_sql("Feautre_Engineering.sql"),  # same file name as in GCS
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )

    # ✅ DAG dependencies
    silver_transform >> gold_transform >> feature_engineering
