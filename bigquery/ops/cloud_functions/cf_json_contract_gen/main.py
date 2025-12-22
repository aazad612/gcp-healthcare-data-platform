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
    if not blob_name.lower().endswith(('.yaml', '.yml')):
        return False

    path_parts = blob_name.split('/')
    if len(path_parts) < 5:
        return False

    # Path: [domain]/[unit]/bronze/ingestion_configs/[table].yaml
    if path_parts[2] == 'bronze' and path_parts[3] == 'ingestion_configs':
        context.domain = path_parts[0]
        context.unit = path_parts[1]
        context.table_name = path_parts[4].rsplit('.', 1)[0]
        return True
    return False

def mapping_exists_in_bq(context):
    client = bigquery.Client(project=context.shared_project)
    query = f"""
        SELECT gcs_yaml_ingestion_config
        FROM `{context.shared_project}.ops_metadata.file_ingestion_mapping`
        WHERE domain = @domain
          AND system_name = @unit
          AND target_table_name = @table
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("domain", "STRING", context.domain),
            bigquery.ScalarQueryParameter("unit", "STRING", context.unit),
            bigquery.ScalarQueryParameter("table", "STRING", context.table_name),
        ]
    )
    results = list(client.query(query, job_config=job_config).result())
    
    if len(results) != 1:
        logger.error(f"Validation Failed: Found {len(results)} mappings for {context.table_name}")
        return False

    context.gcs_json_path = results[0].gcs_yaml_ingestion_config
    return True

def convert_and_upload_json(yaml_content, context):
    try:
        full_contract = yaml.safe_load(yaml_content)
        yaml_columns = full_contract.get('schema', {}).get('columns', [])
        
        json_columns = []
        for col in yaml_columns:
            json_columns.append({
                "name": col.get('name', '').lower(),
                "type": col.get('type', 'STRING'),
                "mode": col.get('mode', 'NULLABLE'),
                "description": col.get('description', '')
            })

        json_payload = json.dumps(json_columns, indent=2)
        
        path_parts = context.gcs_json_path.replace("gs://", "").split("/", 1)
        storage_client = storage.Client()
        bucket = storage_client.bucket(path_parts[0])
        blob = bucket.blob(path_parts[1])
        blob.upload_from_string(json_payload, content_type='application/json')
        
        logger.info(f"✅ Contract deployed: {context.gcs_json_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Conversion failed: {str(e)} pass")
        return False
        

@functions_framework.cloud_event
def contract_processor_trigger(cloud_event):
    data = cloud_event.data
    # Determine execution project from Env Var (set by GitHub Action)
    shared_project = os.environ.get('SHARED_PROJECT')
    
    ctx = MetadataContext(shared_project=shared_project)

    if not is_valid_contract_upload(data["name"], ctx):
        return 

    if not mapping_exists_in_bq(ctx):
        return 

    storage_client = storage.Client()
    blob = storage_client.bucket(data["bucket"]).blob(data["name"])
    yaml_content = blob.download_as_text()

    convert_and_upload_json(yaml_content, ctx)