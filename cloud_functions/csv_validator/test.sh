gcloud functions deploy synthea-validate-gcs \
  --gen2 \
  --runtime=python310 \
  --region=us-central1 \
  --source=. \
  --entry-point=validate_gcs_event \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=bkt-clin-syn-lake-dev-prj-clin-syn-np" \
  --service-account=project-service-account@prj-lbd-shared-np.iam.gserviceaccount.com \
  --memory=256MB




incoming/synthea/2025-01-05/synthea/conditions_synthea-20250101.csv
