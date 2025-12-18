import os
import re
from datetime import datetime
from google.cloud import bigquery
from googleapiclient.discovery import build
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s:%(funcName)s → %(message)s"
)

logger = logging.getLogger(__name__)
bq_client = bigquery.Client()

def only_process_data_files(ctx):
    path = ctx.file_path
    
    # Check 1: Prefix
    if not path.startswith("incoming/"):
        ctx.add_error("File is not an INCOMING Datafile / Skipping execution")
        return False
    else:
        logger.info("Data File Received")
        return True


def file_validity_prechecks(ctx):
    """
    Checks: Prefix 'incoming/', Directory Depth, Date Formats in path.
    Updates: ctx.domain, ctx.system, ctx.table_name
    """

    logger = logging.getLogger(f"steps.{file_validity_prechecks.__name__}")

    path = ctx.file_path
    
    parts = path.split('/')
    if len(parts) < 5:
        ctx.add_error("Path depth too shallow (expected 5 levels)")
        return False

    # Extract Metadata
    ctx.domain = parts[1]
    logger.info(f'The domain is {ctx.domain}')
    ctx.system = parts[3]
    logger.info(f'The system name is {ctx.system}')

    # Check 2: Receipt Date
    receipt_date_string = parts[2]

    try:
        ctx.receipt_date = datetime.strptime(receipt_date_string, "%Y%m%d").strftime("%Y-%m-%d")
        logger.info(f'The receipt date is {ctx.receipt_date}')
    except ValueError:
        ctx.add_error(f"Invalid Folder Date: {receipt_date_string}")
        return False

    # Get tablename and file_date 
    filename = parts[4]

    # Check 3: Filename Structure (Hyphen)
    if '-' not in filename:
        ctx.add_error("Filename missing hyphen separator for table name")
        return False

    ctx.table_name = filename.split('-', 1)[0]
    ctx.file_date = filename.split('-', 1)[1].split('.')[0]
    detected_extension = filename.split('-', 1)[1].split('.')[1]

    
    logger.info(f'Table Name is {ctx.table_name} with data from {ctx.file_date}')


    """
    Queries BigQuery for config. Validates Regex.
    Updates: ctx.bq_config
    """
    query = f"""
        SELECT *
        FROM `{os.environ['OPS_PROJECT']}.ops_metadata.file_ingestion_mapping`
        WHERE bucket_name = @bucket
          AND domain = @domain
          AND system_name = @system
          AND target_table_name = @table
          AND orchestration_mode = 'CLOUDFUNCTION'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("bucket", "STRING", ctx.bucket),
            bigquery.ScalarQueryParameter("domain", "STRING", ctx.domain),
            bigquery.ScalarQueryParameter("system", "STRING", ctx.system),
            bigquery.ScalarQueryParameter("table", "STRING", ctx.table_name),
        ]
    )
    
    results = list(bq_client.query(query, job_config=job_config))
    
    if not results:
        ctx.add_error(f"No configuration found for table: {ctx.table_name}")
        return False

    # Regex Match (Using our split logic)
    config = results[0]
    regex = _convert_pattern_to_regex(config.filename_pattern)
    
    if not re.match(regex, ctx.file_path):
        ctx.add_error(f"Filename pattern mismatch. Expected format: {config.filename_pattern}")
        return False

    ctx.bq_config = config
    # number of rows to validate for this table 
    ctx.row_limit = config.validation_row_count
    ctx.delimiter = config.delimiter
    if config.file_type != detected_extension:
        ctx.add_error ('format expected is {config.file_type} found {detected_extension} instead ')

    ctx.contract_gcs_path = config.contract_gcs_path
    return True


def _convert_pattern_to_regex(db_pattern):
    # (Reuse the robust split logic we wrote earlier)
    parts = re.split(r'(<[^>]+>)', db_pattern)
    regex_parts = []
    for part in parts:
        if part.startswith('<') and part.endswith('>'):
            if 'yyyy-mm-dd' in part.lower(): regex_parts.append(r'\d{4}-\d{2}-\d{2}')
            elif 'yyyymmdd' in part.lower(): regex_parts.append(r'\d{8}')
            else: regex_parts.append(r'[^/]+')
        else:
            regex_parts.append(re.escape(part))
    return f"^{''.join(regex_parts)}$"


# ---------------------------------------------------------
# AUDIT ENTRY
# ---------------------------------------------------------
def make_audit_entry(ctx):
    """
    Writes context state to BigQuery Audit table.
    """
    row = {
        "ingestion_id": ctx.ingestion_id,
        "bucket": ctx.bucket,
        "object_path": ctx.file_path,
        "file_name": ctx.file_path.split('/')[-1],
        "domain": ctx.domain,
        "system_name": ctx.system,
        "validation_status": ctx.status,
        "validation_errors": ctx.errors,
        "file_arrival_time": ctx.time_created,
        "ingestion_started": datetime.utcnow().isoformat(),
        # ... Add other fields from schema ...
    }
    
    table_ref = f"{os.environ['OPS_PROJECT']}.ops_metadata.file_ingestion_audit"
    errors = bq_client.insert_rows_json(table_ref, [row])
    
    if errors:
        logger.error(f"Audit Log Failed: {errors}")

# ---------------------------------------------------------
# STEP 5: CALL DATAFLOW
# ---------------------------------------------------------
def call_dataflow(ctx):
    """
    Constructs arguments and launches the job.
    """
    config = ctx.bq_config
    job_name = f"ingest-{ctx.table_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    service = build('dataflow', 'v1b3', cache_discovery=False)
    
    body = {
        "jobName": job_name,
        "gcsPath": config.dataflow_template_gcs,
        "parameters": {
            "config_file": config.runtime_config_json,
            "input_file": f"gs://{ctx.bucket}/{ctx.file_path}"
        },
        "environment": {
            "tempLocation": f"gs://{os.environ.get('TEMP_BUCKET')}/temp",
            "zone": "us-east1-b"
        }
    }
    
    req = service.projects().templates().launch(
        projectId=os.environ['OPS_PROJECT'],
        body=body
    )
    req.execute()
    logger.info(f"Dataflow Launched: {job_name}")