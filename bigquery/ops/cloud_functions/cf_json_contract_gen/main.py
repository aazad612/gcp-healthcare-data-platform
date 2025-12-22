import functions_framework
import json
import yaml
import logging
import os
import re
from google.cloud import storage, bigquery

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetadataContext:
    def __init__(self, domain=None, unit=None, table_name=None, shared_project=None, gcs_json_path=None):
        self.domain = domain
        self.unit = unit
        self.table_name = table_name
        self.shared_project = shared_project
        self.gcs_json_path = gcs_json_path

def is_valid_contract_upload(blob_name, context):
    """STEP 1: Path Validation and Entity Normalization"""
    logger.info(f"[STEP 1] Validating upload path: {blob_name}")
    
    if not blob_name.lower().endswith(('.yaml', '.yml')):
        logger.info(f"  --> Skip: File is not YAML.")
        return False

    path_parts = blob_name.split('/')
    if len(path_parts) < 5:
        logger.info(f"  --> Skip: Path depth too shallow ({len(path_parts)} parts).")
        return False

    # Expected: [domain]/[unit]/bronze/ingestion_configs/[table_v1].yaml
    if path_parts[2] == 'bronze' and path_parts[3] == 'ingestion_configs':
        context.domain = path_parts[0]
        context.unit = path_parts[1]
        
        # Strip extension
        raw_filename = path_parts[4].rsplit('.', 1)[0]
        
        # STRIP VERSION: 'encounters_v1' -> 'encounters'
        # Matches '_v' followed by digits at the end of the string
        context.table_name = re.sub(r'_v\d+$', '', raw_filename)
        
        logger.info(f"  --> SUCCESS: Parsed Domain='{context.domain}', Unit='{context.unit}', Entity='{context.table_name}'")
        return True
        
    logger.info(f"  --> Skip: Path structure does not match ingestion_configs pattern.")
    return False

def mapping_exists_in_bq(context):
    """STEP 2: Metadata Lookup in BigQuery"""
    logger.info(f"[STEP 2] Looking up mapping for entity '{context.table_name}' in BQ...")
    
    client = bigquery.Client(project=context.shared_project)
    
    query = f"""
        SELECT gcs_json_contract
        FROM `{context.shared_project}.ops_metadata.file_ingestion_mapping`
        WHERE domain = @domain
          AND system_name = @unit
          AND entity = @table
          AND env = 'dev'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("domain", "STRING", context.domain),
            bigquery.ScalarQueryParameter("unit", "STRING", context.unit),
            bigquery.ScalarQueryParameter("table", "STRING", context.table_name),
        ]
    )
    
    try:
        results = list(client.query(query, job_config=job_config).result())
    except Exception as e:
        logger.error(f"  --> BQ QUERY CRASHED: {str(e)}")
        raise

    if len(results) == 0:
        raise ValueError(
            f"  --> FAILURE: 0 rows found for Domain={context.domain}, Unit={context.unit}, Entity={context.table_name}. "
            f"Verify that the 'entity' column in BQ does NOT have a '_v1' suffix."
        )
    
    context.gcs_json_path = results[0].gcs_json_contract
    logger.info(f"  --> SUCCESS: Found target JSON path: {context.gcs_json_path}")
    return True

def convert_and_upload_json(yaml_content, context):
    """STEP 3: YAML to JSON Transformation and GCS Upload"""
    logger.info(f"[STEP 3] Converting YAML content for '{context.table_name}'...")
    
    try:
        full_contract = yaml.safe_load(yaml_content)
        yaml_columns = full_contract.get('schema', {}).get('columns', [])
        
        if not yaml_columns:
            raise KeyError(f"The YAML for {context.table_name} is missing the 'schema.columns' key.")

        json_columns = []
        for col in yaml_columns:
            json_columns.append({
                "name": col.get('name', '').lower(),
                "type": col.get('type', 'STRING'),
                "mode": col.get('mode', 'NULLABLE'),
                "description": col.get('description', '')
            })

        json_payload = json.dumps(json_columns, indent=2)
        logger.info(f"  --> JSON payload generated ({len(json_columns)} columns).")
        
        # Upload to bkt-clin-syn-configs-np
        storage_client = storage.Client()
        bucket = storage_client.bucket('bkt-clin-syn-configs-np')
        blob = bucket.blob(context.gcs_json_path)
        
        logger.info(f"  --> Uploading to gs://bkt-clin-syn-configs-np/{context.gcs_json_path}...")
        blob.upload_from_string(json_payload, content_type='application/json')
        
        logger.info(f"  --> SUCCESS: JSON Contract deployed.")
        
    except Exception as e:
        logger.error(f"  --> CONVERSION/UPLOAD FAILED: {str(e)}")
        raise


@functions_framework.cloud_event
def contract_processor_trigger(cloud_event):
    """MAIN ENTRY POINT"""
    data = cloud_event.data
    shared_project = os.environ.get('SHARED_PROJECT')
    
    logger.info("**************************************************")
    logger.info(f"STARTING PROCESSING: {data['name']}")
    logger.info("**************************************************")
    
    ctx = MetadataContext(shared_project=shared_project)

    # 1. Validate and Parse
    if not is_valid_contract_upload(data["name"], ctx):
        return 

    # 2. Map to Metadata
    mapping_exists_in_bq(ctx)

    # 3. Read Source YAML
    logger.info(f"READING SOURCE: gs://{data['bucket']}/{data['name']}")
    storage_client = storage.Client()
    blob = storage_client.bucket(data["bucket"]).blob(data["name"])
    yaml_content = blob.download_as_text()

    # 4. Transform and Upload
    convert_and_upload_json(yaml_content, ctx)
    
    logger.info(f"FINISH: All steps completed for {ctx.table_name}.")