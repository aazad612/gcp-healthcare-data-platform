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







locals {
  default_apis = [
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "cloudbuild.googleapis.com",
    "serviceusage.googleapis.com",
    "storage-component.googleapis.com",
    "eventarc.googleapis.com",
    "pubsub.googleapis.com",
    "compute.googleapis.com" ]
}


module "service_projects" {
  source  = "terraform-google-modules/project-factory/google"
  version = "~> 15.0"

  for_each = var.service_projects

  name       = each.value.project_id
  project_id = each.value.project_id
  org_id     = var.org_id

  folder_id = local.folder_ids[each.value.folder_key]

  billing_account = var.billing_account_id

  svpc_host_project_id = local.host_project_ids[each.value.host_project_key]

  sa_role = "roles/editor"

  shared_vpc_subnets = [
    local.subnet_ids[each.value.subnet_key]
  ]



  # DYNAMIC CONFIGURATION
  # 1. Use the specific list from tfvars
  activate_apis = distinct(concat(local.default_apis, each.value.apis))

  # 2. (Optional) Disable the default APIs if you want strict control
  # If false, the module automatically adds compute, container, etc.
  disable_services_on_destroy = false
}

variable "project_sa_roles" {
  type = list(string)
  default = [
    "roles/bigquery.jobUser",
    "roles/bigquery.dataOwner",
    "roles/bigquery.admin",
    "roles/storage.objectAdmin",
    "roles/pubsub.admin",
    "roles/eventarc.publisher",
    "roles/eventarc.eventReceiver",
    "roles/iam.serviceAccountUser"
  ]
}

locals {
  project_sa_role_bindings = {
    for item in flatten([
      for proj_key, proj in var.service_projects : [
        for role in var.project_sa_roles : {
          key        = "${proj_key}-${role}"
          project_id = proj.project_id
          role       = role
        }
      ]
    ]) : item.key => item
  }
}


resource "google_project_iam_member" "project_sa_roles" {
  for_each = local.project_sa_role_bindings

  project = each.value.project_id
  role    = each.value.role
  member  = "serviceAccount:project-service-account@${each.value.project_id}.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "shared_bq_user" {
  for_each = var.service_projects

  project = "prj-lbd-shared-np"
  role    = "roles/bigquery.user"
  member  = "serviceAccount:project-service-account@${each.value.project_id}.iam.gserviceaccount.com"
}


# Data source to fetch project number for all NP service projects (used for SA construction)
data "google_project" "np_service_projects_info" {
  for_each = local.np_service_projects
  project_id = each.value.project_id
}

# Data source to fetch project number for all PD service projects
data "google_project" "pd_service_projects_info" {
  for_each = local.pd_service_projects
  project_id = each.value.project_id
}


resource "google_project_iam_member" "np_gcs_eventarc_publisher" {
  for_each = local.np_service_projects

  project = each.value.project_id
  
  role = "roles/pubsub.publisher"

  member = "serviceAccount:service-${data.google_project.np_service_projects_info[each.key].number}@gs-project-accounts.iam.gserviceaccount.com"
  
}



resource "google_project_iam_member" "pd_gcs_eventarc_publisher" {
  for_each = local.pd_service_projects

  project = each.value.project_id
  role    = "roles/pubsub.publisher"

  member = "serviceAccount:service-${data.google_project.pd_service_projects_info[each.key].number}@gs-project-accounts.iam.gserviceaccount.com"
}


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


