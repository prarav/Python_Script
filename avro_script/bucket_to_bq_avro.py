from google.cloud import storage, bigquery
import os

# ✅ Your GCP config
PROJECT_ID = "prj-kb-prd-looker-gcp-1014"
DATASET_ID = "lrcDB"
BUCKET_NAME = "lrc-db-bucket"
GCS_PREFIX = "mariadb_exports/"  # Folder prefix in bucket

# ✅ Initialize clients
storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)

# ✅ List all .avro files in the bucket folder
blobs = storage_client.list_blobs(BUCKET_NAME, prefix=GCS_PREFIX)
avro_files = [blob.name for blob in blobs if blob.name.endswith(".avro")]

# ✅ Load each .avro file into BigQuery
for blob_name in avro_files:
    file_name = os.path.basename(blob_name)
    table_name = file_name.replace(".avro", "")
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"

    print(f"🚀 Loading {file_name} into {table_id}...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"✅ Done: {file_name} → {table_id}")

print("🎉 All files loaded successfully.")
