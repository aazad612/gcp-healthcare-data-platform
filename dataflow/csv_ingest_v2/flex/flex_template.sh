
# GARBAGE FROM CHATGPT 
# Local testing 

------------------------------------------------------------------------------------------------------------

python main.py \
  --input_file gs://bkt-.../incoming/.../file.csv \
  --runner DirectRunner

------------------------------------------------------------------------------------------------------------

python main.py \
  --input_file gs://bkt-.../incoming/.../file.csv \
  --runner DataflowRunner \
  --project <DATAFLOW_PROJECT> \
  --region us-central1 \
  --temp_location gs://<temp-bucket>/dataflow/tmp \
  --staging_location gs://<staging-bucket>/dataflow/staging \
  --subnetwork https://www.googleapis.com/compute/v1/projects/<host-project>/regions/us-central1/subnetworks/<subnet-name> \
  --service_account_email <dataflow-sa>@<project>.iam.gserviceaccount.com \
  --no_use_public_ips


------------------------------------------------------------------------------------------------------------


export REGION=us-central1
export PROJECT=prj-lbd-shared-np
export REPO=dataflow-templates
export IMAGE_NAME=ingestion
export TAG=v1

gcloud auth configure-docker ${REGION}-docker.pkg.dev

docker build -t ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${IMAGE_NAME}:${TAG} .
docker push ${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${IMAGE_NAME}:${TAG}


export TEMPLATE_PATH="gs://bkt-df-templates-np/templates/ingestion.json"

gcloud dataflow flex-template build ${TEMPLATE_PATH} \
  --image "${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${IMAGE_NAME}:${TAG}" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json"


gcloud dataflow flex-template run "ingest-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location="${TEMPLATE_PATH}" \
  --region="${REGION}" \
  --parameters input_file="incoming/clinical/20251229/synthea/patient-20251229.csv",env="dev"


