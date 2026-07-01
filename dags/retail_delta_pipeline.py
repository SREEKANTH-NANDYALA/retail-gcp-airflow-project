from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

PROJECT_ID = "my-retail-project-123456"
DATASET = "retail_dw"
BUCKET = "retail-orders-sreekanth"

with DAG(
    dag_id="retail_delta_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="0 */2 * * *",      # Every 2 hours
    catchup=False,
    tags=["retail", "gcp", "bigquery"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket=BUCKET,

        # Load every CSV in incoming/
        source_objects=[
            "incoming/*.csv"
        ],

        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.orders_staging",

        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,

        write_disposition="WRITE_TRUNCATE",

        create_disposition="CREATE_IF_NEEDED",
    )

    insert_new_orders = BigQueryInsertJobOperator(
        task_id="insert_new_orders",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET}.new_orders`

                SELECT
                    s.*,
                    CURRENT_TIMESTAMP() AS inserted_at

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
                    CURRENT_TIMESTAMP() AS inserted_at

                FROM `{PROJECT_ID}.{DATASET}.orders_staging` s
                """,
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> load_to_staging >> insert_new_orders >> update_raw >> end