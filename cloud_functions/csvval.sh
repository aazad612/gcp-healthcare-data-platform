gcloud functions deploy ingest-validator-dev \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=./csv_validator \
  --entry-point=ingest_validator \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=bkt-clin-syn-lake-dev-prj-clin-syn-np" \
  --trigger-location=us \
  --set-env-vars=CONFIG_BUCKET=bkt-clin-syn-configs-np \
  --set-env-vars=AUDIT_TABLE=prj-lbd-shared-np.ops_metadata.file_ingestion_audit \
  --run-service-account="project-service-account@prj-clin-syn-np.iam.gserviceaccount.com" \
  --service-account="project-service-account@prj-clin-syn-np.iam.gserviceaccount.com" \
  --build-service-account="projects/prj-clin-syn-np/serviceAccounts/project-service-account@prj-clin-syn-np.iam.gserviceaccount.com" \
  --project=prj-clin-syn-np


gcloud functions call ingest-validator-dev \
    --region=us-central1 \
    --project=prj-clin-syn-np \
    --data '{
      "bucket": "bkt-clin-syn-lake-dev-prj-clin-syn-np",
      "name": "incoming/synthea/2025-01-05/synthea/encounters_synthea-20250101.csv",
      "eventType": "google.cloud.storage.object.v1.finalized"
    }'

  --service-account="projects/prj-clin-syn-np.serviceAccounts/project-service-account@prj-clin-syn-pd.iam.gserviceaccount.com" \


#   --build-service-account=project-service-account@prj-clin-syn-np.iam.gserviceaccount.com \
--trigger-service-account



# Replace [PROJECT_NUMBER] with the actual project number of prj-clin-syn-np
GCP_PROJECT_ID="prj-clin-syn-np" 
GCP_PROJECT_NUMBER=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectNumber)")
SERVICE_AGENT="service-${GCP_PROJECT_NUMBER}@gcf-admin-robot.iam.gserviceaccount.com"

# Grant the default administrative role to the service agent
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
    --member="serviceAccount:${SERVICE_AGENT}" \
    --role="roles/cloudfunctions.serviceAgent"

Cloud Pub/Sub needs the role roles/iam.serviceAccountTokenCreator granted to service account service-629417836000@gcp-sa-pubsub.iam.gserviceaccount.com on this project to create identity tokens. You can change this later.

This trigger needs the role roles/pubsub.publisher granted to service account service-629417836000@gs-project-accounts.iam.gserviceaccount.com to receive events via Cloud Storage.


gcloud projects add-iam-policy-binding PROJECT_ID \
    --member=serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com \
    --role=roles/eventarc.eventReceiver

SERVICE_ACCOUNT="$(gcloud storage service-agent --project=PROJECT_ID)"

gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role='roles/pubsub.publisher'

gcloud projects add-iam-policy-binding PROJECT_ID \
    --member=serviceAccount:service-PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com \
    --role=roles/iam.serviceAccountTokenCreator



gsutil cp encounters1.csv gs://bkt-clin-syn-lake-dev-prj-clin-syn-np/incoming/synthea/2025-01-05/synthea/encounters_synthea-20250101.csv



gs://bkt-clin-syn-configs-np/contracts/encounters_v1.json
gs://bkt-clin-syn-configs-np/bronze/contracts/encounters_v1.json




gcloud beta services identity create \
    --project=prj-clin-syn-pd \
    --service=storage.googleapis.com


gcloud beta services identity create \
    --project=prj-lbd-shared-pd \
    --service=storage.googleapis.com



gcloud iam service-accounts enable 801518458560-compute@developer.gserviceaccount.com --project prj-lbd-shared-pd


prj-clin-syn-pd (Project Number 795822506118)

prj-lbd-shared-pd (Project Number 801518458560)

bq get-iam-policy --format=json prj-lbd-shared-np:ops_metadata > policy.json




(mypython) ~/work/gcp-healthcare-data-engineering-platform/infra/06-workloads/shared_ops_new [main] % terraform state rm 'google_pubsub_topic.df_ingest_topic["np"]'

Removed google_pubsub_topic.df_ingest_topic["np"]
Successfully removed 1 resource instance(s).
(mypython) ~/work/gcp-healthcare-data-engineering-platform/infra/06-workloads/shared_ops_new [main] % terraform state rm 'google_pubsub_topic.df_ingest_topic["pd"]'

Removed google_pubsub_topic.df_ingest_topic["pd"]
Successfully removed 1 resource instance(s).

(mypython) ~/work/gcp-healthcare-data-engineering-platform/infra/06-workloads/shared_ops_new [main] % terraform state rm 'google_pubsub_subscription.df_ingest_sub["np"]'

terraform state rm 'google_pubsub_subscription.df_ingest_sub["pd"]'
Removed google_pubsub_subscription.df_ingest_sub["np"]

Successfully removed 1 resource instance(s).
(mypython) ~/work/gcp-healthcare-data-engineering-platform/infra/06-workloads/shared_ops_new [main] % terraform state rm 'google_pubsub_subscription.df_ingest_sub["pd"]'
Removed google_pubsub_subscription.df_ingest_sub["pd"]
Successfully removed 1 resource instance(s).