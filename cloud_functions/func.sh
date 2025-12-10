# 1. Get the Function URL
FUNCTION_URL=$(gcloud functions describe ingest-validator-dev \
    --region=us-central1 \
    --project=prj-clin-syn-np \
    --format="value(serviceConfig.uri)")

# 2. Get an Identity Token
TOKEN=$(gcloud auth print-identity-token)

# 3. Send a Valid CloudEvent
curl -X POST "$FUNCTION_URL" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "ce-id: 1234567890" \
  -H "ce-source: //storage.googleapis.com/projects/_/buckets/bkt-clin-syn-lake-dev-prj-clin-syn-np" \
  -H "ce-specversion: 1.0" \
  -H "ce-type: google.cloud.storage.object.v1.finalized" \
  -d '{
        "bucket": "bkt-clin-syn-lake-dev-prj-clin-syn-np",
        "name": "incoming/synthea/2025-01-05/synthea/encounters_synthea-20250101.csv"
      }'
