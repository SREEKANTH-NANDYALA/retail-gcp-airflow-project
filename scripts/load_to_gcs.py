from google.cloud import storage

BUCKET_NAME = "retail-orders-sreekanth"
SOURCE_FILE = "data/day1_orders.csv"
DESTINATION_BLOB = "incoming/day1_orders.csv"


def upload_to_gcs(bucket_name, source_file, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(source_file)

    print(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")


if __name__ == "__main__":
    upload_to_gcs(BUCKET_NAME, SOURCE_FILE, DESTINATION_BLOB)