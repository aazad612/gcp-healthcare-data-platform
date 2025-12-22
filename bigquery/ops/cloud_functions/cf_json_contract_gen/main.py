import functions_framework
import json
import yaml
import logging
import os
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
    """Filters events. Returns False if file is irrelevant to stop execution quietly."""
    if not blob_name.lower().endswith(('.yaml', '.yml')):
        return False

    path_parts = blob_name.split('/')
    if len(path_parts) < 5:
        return False

    # Path check: [domain]/[unit]/bronze/ingestion_configs/[table].yaml
    if path_parts[2] == 'bronze' and path_parts[3] == 'ingestion_configs':
        context.domain = path_parts[0]
        context.unit = path_parts[1]
        context.table_name = path_parts[4].rsplit('.', 1)[0]
        return True
    return False

def mapping_exists_in_bq(context):
    """Verifies SQL mapping. Raises ValueError on failure to ensure visibility."""
    client = bigquery.Client(project=context.shared_project)
    
    # Root Cause Identification: Query specifically for the gcs_json_contract path
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
    
    results = list(client.query(query, job_config=job_config).result())
    
    if len(results) == 0:
        raise ValueError(
            f"SQL LOOKUP FAILURE: No mapping found in {context.shared_project}.ops_metadata.file_ingestion_mapping "
            f"for domain='{context.domain}', system_name='{context.unit}', entity='{context.table_name}'"
        )
    
    if len(results) > 1:
        raise ValueError(
            f"SQL DUPLICATE FAILURE: Found {len(results)} rows for entity '{context.table_name}'. "
            f"Cleanup required in metadata table."
        )

    # Use the column from your SQL: gcs_json_contract
    context.gcs_json_path = results[0].gcs_json_contract
    return True

def convert_and_upload_json(yaml_content, context):
    """Converts YAML to JSON. Re-raises exceptions to prevent silent failures."""
    try:
        full_contract = yaml.safe_load(yaml_content)
        if not full_contract:
            raise ValueError("YAML parsing yielded empty content.")
            
        yaml_columns = full_contract.get('schema', {}).get('columns', [])
        if not yaml_columns:
            raise KeyError(f"Missing 'schema.columns' key in YAML for {context.table_name}")
        
        json_columns = []
        for col in yaml_columns:
            json_columns.append({
                "name": col.get('name', '').lower(),
                "type": col.get('type', 'STRING'),
                "mode": col.get('mode', 'NULLABLE'),
                "description": col.get('description', '')
            })

        json_payload = json.dumps(json_columns, indent=2)
        
        # Split bucket and path for upload
        path_parts = context.gcs_json_path.split("/", 1)
        storage_client = storage.Client()
        bucket = storage_client.bucket('bkt-clin-syn-configs-np') # Target config bucket
        blob = bucket.blob(path_parts[1] if len(path_parts) > 1 else path_parts[0])
        
        blob.upload_from_string(json_payload, content_type='application/json')
        logger.info(f"✅ Successfully deployed JSON contract to: gs://bkt-clin-syn-configs-np/{blob.name}")
        
    except Exception as e:
        logger.error(f"CRITICAL CONVERSION ERROR for {context.table_name}: {str(e)}")
        raise # Re-raise to crash the function and trigger a Traceback in logs

@functions_framework.cloud_event
def contract_processor_trigger(cloud_event):
    data = cloud_event.data
    shared_project = os.environ.get('SHARED_PROJECT')
    
    ctx = MetadataContext(shared_project=shared_project)

    # Quiet exit if the file uploaded isn't a YAML config
    if not is_valid_contract_upload(data["name"], ctx):
        return 

    # All subsequent steps raise exceptions on failure
    mapping_exists_in_bq(ctx)

    storage_client = storage.Client()
    blob = storage_client.bucket(data["bucket"]).blob(data["name"])
    yaml_content = blob.download_as_text()

    convert_and_upload_json(yaml_content, ctx)