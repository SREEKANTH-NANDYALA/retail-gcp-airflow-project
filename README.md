# Retail GCP Airflow Project

This project demonstrates an end-to-end retail data pipeline on Google Cloud Platform.

## Technologies Used

- Python
- Google Cloud Storage
- BigQuery
- Cloud Composer (Airflow)
- Pub/Sub
- GitHub

## Workflow

```text
CSV Files
    ↓
Cloud Storage
    ↓
Airflow DAG
    ↓
BigQuery Staging
    ↓
Delta Detection
    ↓
Final Table
    ↓
Streaming Events via Pub/Sub
```