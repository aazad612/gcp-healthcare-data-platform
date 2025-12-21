import yaml
import json
import logging
from google.cloud import bigquery
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
ORG_DF_VARS_FILE = 'org_df_vars.yaml'

def fetch_standards(shared_project, standard_id='BRONZE_V1'):
    """
    Fetches the authoritative list of Meta Columns and Rules from BigQuery.
    """
    client = bigquery.Client(project=shared_project)
    
    query = f"""
        SELECT column_name, expected_type, is_mandatory
        FROM `{shared_project}.ops_metadata.standards_definition`
        WHERE standard_id = '{standard_id}'
    """
    
    standards = {}
    results = client.query(query).result()
    
    for row in results:
        standards[row.column_name] = {
            'type': row.expected_type,
            'mandatory': row.is_mandatory
        }
    
    if not standards:
        raise ValueError(f"CRITICAL: No standards found for ID '{standard_id}' in {shared_project}")
        
    logging.info(f"Loaded {len(standards)} Governance Rules for {standard_id}")
    return standards


def load_org_config(domain, unit, env):
    """
    Loads infrastructure configuration from the local org_df_vars.yaml file.
    """
    logger.info(f"Loading Org Config for {domain}.{unit} in {env}...")
    try:
        with open(ORG_DF_VARS_FILE) as f:
            master = yaml.safe_load(f)

        d_block = master["domains"][domain]
        shared  = d_block["shared_infra"]
        u_block = d_block["units"][unit]

        return {
            "domain": domain,
            "unit": unit,
            "env": env,
            "region": d_block["region"],
            "use_public_ips": d_block["use_public_ips"],
            "shared_project": shared["project"][env],
            "dataflow_sa": shared["dataflow_sa"][env],
            "temp_bucket": shared["temp_bucket"][env],
            "staging_bucket": shared["staging_bucket"][env],
            "subnetwork": shared["subnet"][env],
            "service_project": u_block["project"][env],
            "service_project_sa": u_block["sa"][env],
            "dataset": u_block["dataset"][env],
            "config_bucket": u_block["config_bucket"][env],
            "landing_bucket": u_block["landing_bucket"][env]
        }
    except KeyError as e:
        logger.error(f"Configuration not found for {domain}.{unit} in {env}: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"Configuration file {ORG_DF_VARS_FILE} not found.")
        raise


def load_mapping_table_info(shared_project, domain, unit, table_name):
    """
    Queries the BigQuery Mapping Table in the Shared Ops Project.
    """
    logger.info(f"Querying Mapping Table for {table_name}...")
    client = bigquery.Client(project=shared_project)
    
    query = f"""
        SELECT *
        FROM `{shared_project}.ops_metadata.file_ingestion_mapping`
        WHERE domain = @domain
          AND system_name = @unit
          AND target_table_name = @table
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("domain", "STRING", domain),
            bigquery.ScalarQueryParameter("unit", "STRING", unit),
            bigquery.ScalarQueryParameter("table", "STRING", table_name),
        ]
    )

    results = list(client.query(query, job_config=job_config))

    if not results:
        error_msg = f"No mapping found for table: {domain}.{unit}.{table_name}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    row = dict(results[0])
    logger.info(f"Found mapping config. GCS Contract: {row.get('gcs_yaml_ingestion_config')}")
    return row


def load_ingestion_contract(gcs_path):
    """
    Downloads and parses the YAML Data Contract from GCS.
    """
    logger.info(f"Loading Data Contract from {gcs_path}...")
    
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS Path: {gcs_path}")

    path_parts = gcs_path.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    blob_name = path_parts[1]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        content = blob.download_as_text()
        contract = yaml.safe_load(content)
        return contract
    except Exception as e:
        logger.error(f"Failed to load contract from GCS: {e}")
        raise


def get_metadata(domain, unit, table_name, env):
    """
    Master function called by Dataflow.
    Aggregates Infra Config, Orchestration Rules, Data Contract, and Standards.
    """
    logger.info(f"🚀 Starting Metadata Aggregation for {domain}.{unit}.{table_name} ({env})")

    # 1. Load Infrastructure Config (Local YAML)
    infra_config = load_org_config(domain, unit, env)

    # 2. Load Mapping Info (BigQuery)
    mapping_info = load_mapping_table_info(
        shared_project=infra_config['shared_project'],
        domain=domain,
        unit=unit,
        table_name=table_name
    )

    # 3. Load Data Contract (GCS)
    contract_path = mapping_info.get('gcs_yaml_ingestion_config')
    if not contract_path:
        raise ValueError("Mapping table missing 'gcs_yaml_ingestion_config'")
    data_contract = load_ingestion_contract(contract_path)

    # 4. Fetch Governance Standards (BigQuery)
    # Uses standard_id from mapping if present, otherwise defaults to BRONZE_V1
    std_id = mapping_info.get('standard_id', 'BRONZE_V1')
    governance_standards = fetch_standards(
        shared_project=infra_config['shared_project'], 
        standard_id=std_id
    )

    # 5. Merge into Master Metadata Object
    metadata = {
        "context": {
            "domain": domain,
            "unit": unit,
            "table_name": table_name,
            "env": env
        },
        "infrastructure": infra_config,
        "orchestration": mapping_info,
        "contract": data_contract,
        "governance": governance_standards
    }

    logger.info("✅ Metadata Aggregation Complete.")
    return metadata

if __name__ == "__main__":
    try:
        meta = get_metadata("clinical", "synthea", "conditions", "dev")
        print(json.dumps(meta, indent=2, default=str))
    except Exception as e:
        print(f"Failed: {e}")