gcloud functions deploy ingest-validator-dev \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=. \
  --entry-point=ingest_validator \
  --trigger-event-types=google.cloud.storage.object.v1.finalized \
  --trigger-resource=bkt-clin-syn-lake-dev-prj-clin-syn-np \
  --set-env-vars=CONFIG_BUCKET=bkt-clin-syn-configs-np \
  --set-env-vars=AUDIT_TABLE=prj-lbd-shared-np.ops_metadata.file_ingestion_audit


gsutil cp gs://bkt-clin-syn-lake-dev-prj-clin-syn-np/incoming/synthea/2025-01-05/synthea/conditions_synthea-20250101.csv \
gs://bkt-clin-syn-lake-dev-prj-clin-syn-np/incoming/clinical/20251214/synthea/encounters-2025-01-01.csv

gcloud functions logs read ingest-validator-dev --limit=50


SELECT * 
FROM `prj-lbd-shared-np.ops_metadata.file_ingestion_audit`
ORDER BY ingest_timestamp DESC;


