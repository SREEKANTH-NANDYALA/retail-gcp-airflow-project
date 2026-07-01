from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

PROJECT_ID = "my-retail-project-123456"
DATASET = "retail_dw"
BUCKET = "retail-orders-sreekanth"
INCOMING_PREFIX = "incoming/"


def get_latest_file(**context):
    hook = GCSHook()
    files = hook.list(bucket_name=BUCKET, prefix=INCOMING_PREFIX)

    csv_files = [file for file in files if file.endswith(".csv")]

    if not csv_files:
        raise ValueError("No CSV files found in incoming folder")

    latest_file = sorted(csv_files)[-1]

    context["ti"].xcom_push(key="latest_file", value=latest_file)

    print(f"Latest file found: {latest_file}")


with DAG(
    dag_id="retail_delta_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="0 */2 * * *",
    catchup=False,
    tags=["retail", "gcp", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    find_latest_file = PythonOperator(
        task_id="find_latest_file",
        python_callable=get_latest_file,
    )

    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket=BUCKET,
        source_objects=[
            "{{ ti.xcom_pull(task_ids='find_latest_file', key='latest_file') }}"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.orders_staging",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    insert_new_orders = BigQueryInsertJobOperator(
        task_id="insert_new_orders",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET}.new_orders`
                SELECT
                    s.*,
                    CURRENT_TIMESTAMP() AS detected_at
                FROM `{PROJECT_ID}.{DATASET}.orders_staging` s
                LEFT JOIN `{PROJECT_ID}.{DATASET}.orders_raw` r
                ON s.order_id = r.order_id
                WHERE r.order_id IS NULL
                """,
                "useLegacySql": False,
            }
        },
    )

    update_raw = BigQueryInsertJobOperator(
        task_id="update_raw",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET}.orders_raw`
                SELECT
                    s.*,
                    CURRENT_TIMESTAMP() AS ingestion_date
                FROM `{PROJECT_ID}.{DATASET}.orders_staging` s
                LEFT JOIN `{PROJECT_ID}.{DATASET}.orders_raw` r
                ON s.order_id = r.order_id
                WHERE r.order_id IS NULL
                """,
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(task_id="end")

    start >> find_latest_file >> load_to_staging >> insert_new_orders >> update_raw >> end