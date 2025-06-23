#!/bin/bash

DB_NAME="doptmoodle"
GCS_BUCKET="lrc-db-bucket"
BQ_DATASET="lrcDB"

# Loop through all AVRO files in the GCS bucket path
gsutil ls gs://$GCS_BUCKET/mariadb_exports/*.avro | while read FILE; do
  TABLE_NAME=$(basename "$FILE" .avro)

  # Get file size (in bytes) to skip empty/malformed files
  SIZE=$(gsutil du "$FILE" | awk '{print $1}')
  if [ "$SIZE" -lt 100 ]; then
    echo "⚠️ Skipping $TABLE_NAME — likely empty or malformed ($SIZE bytes)"
    continue
  fi

  echo "✅ Importing $TABLE_NAME from $FILE ($SIZE bytes)..."

  bq load \
    --source_format=AVRO \
    --autodetect \
    --replace \
    "$BQ_DATASET.$TABLE_NAME" \
    "$FILE"

  echo ""
done

echo "✅ All AVRO files processed."
