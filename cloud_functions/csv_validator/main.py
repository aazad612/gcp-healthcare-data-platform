import functions_framework
import json
import re
import os
import uuid  # <-- Added for ingestion_id
from datetime import datetime
from dateutil.parser import parse as parse_dt
from google.cloud import storage
from google.cloud import bigquery

# ----------------------------------------
# CONFIG
# ----------------------------------------
CONFIG_BUCKET = os.environ.get("CONFIG_BUCKET", "bkt-clin-syn-configs-np")
AUDIT_TABLE = os.environ.get("AUDIT_TABLE", "prj-lbd-shared-np.ops_metadata.file_ingestion_audit")
CONTRACTS_PREFIX = "bronze/contracts/"

storage_client = storage.Client()
bq_client = bigquery.Client()

# Regex to capture parts of the filename
FILENAME_PATTERN = re.compile(
    r"^(?P<table>[a-zA-Z0-9]+)_(?P<system>[a-zA-Z0-9]+)-(?P<date>\d{8})\.(csv|json)$"
)

def log_audit_record(bucket, object_path, filename, system_name, entity, file_date, status, error_msg):
    """
    Writes a row to the BigQuery Audit Table matching the PROVIDED SCHEMA.
    """
    
    # 1. Handle Date Conversion (YYYYMMDD -> YYYY-MM-DD)
    try:
        if file_date:
            parsed_date = datetime.strptime(file_date, "%Y%m%d").date().isoformat()
        else:
            parsed_date = datetime.utcnow().date().isoformat() # Fallback
    except ValueError:
        parsed_date = datetime.utcnow().date().isoformat()

    # 2. Handle Validation Errors (Schema says REPEATED STRING)
    # We must send a list/array, not a single string.
    validation_errors_list = [error_msg] if error_msg else []

    # 3. Construct Row based on YOUR Schema
    row = [{
        "ingestion_id": str(uuid.uuid4()),
        "bucket": bucket,
        "object_path": object_path,
        "file_name": filename,
        "entity": entity or "UNKNOWN",
        "system_name": system_name or "UNKNOWN",
        "arrival_date": parsed_date,
        "validation_status": status,
        "validation_errors": validation_errors_list, # <--- FIXED KEY NAME & TYPE
        "meta_ingest_timestamp": datetime.utcnow().isoformat()
    }]
    
    try:
        # insert_rows_json expects a list of dicts
        errors = bq_client.insert_rows_json(AUDIT_TABLE, row)
        if errors:
            print(f"ERROR: BQ Insert Failed: {errors}")
        else:
            print("INFO: Audit record inserted successfully.")
    except Exception as e:
        print(f"ERROR: Failed to write to audit log: {e}")

def validate_contract_exists(system, table):
    """Checks if the schema contract exists in the config bucket."""
    if not table: return False, "Table name is missing"
    
    path = f"{CONTRACTS_PREFIX}{table.lower()}_v1.json"
    bucket = storage_client.bucket(CONFIG_BUCKET)
    blob = bucket.blob(path)

    if not blob.exists():
        return False, f"Contract not found: gs://{CONFIG_BUCKET}/{path}"
    return True, "OK"

@functions_framework.cloud_event
def ingest_validator(cloud_event):
    """Main Entry Point."""
    
    # 1. Extract Data
    data = cloud_event.data

    # Handle Manual Invocation (String input)
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            print(f"CRITICAL: Failed to parse input JSON: {e}")
            return 

    # 2. Extract Basic Fields
    bucket_name = data.get("bucket")
    file_path = data.get("name") # Full object path (e.g. incoming/...)

    if not bucket_name or not file_path:
        print(f"ERROR: Invalid event. Missing bucket/name. Data: {data}")
        return

    # Extract filename from path
    filename = file_path.split("/")[-1]

    print(f"➡️ Processing: gs://{bucket_name}/{file_path}")

    # 3. Path Depth Validation
    parts = file_path.split("/")
    if len(parts) < 5:
        msg = f"Invalid path depth: {file_path}"
        print(f"FAILED: {msg}")
        log_audit_record(bucket_name, file_path, filename, None, None, None, "FAILED", msg)
        return

    # Extract parts based on your folder structure
    # incoming/synthea/<arrival_date>/<system>/<filename>
    _, _, arrival_date_folder, system_name_folder, _ = parts[-5:]

    # 4. Filename Pattern Validation
    m = FILENAME_PATTERN.match(filename)
    if not m:
        msg = f"Filename pattern mismatch: {filename}"
        print(f"FAILED: {msg}")
        log_audit_record(bucket_name, file_path, filename, system_name_folder, None, None, "FAILED", msg)
        return

    table_name = m.group("table")
    file_date = m.group("date") # Extracts '20250101' string

    # 5. Contract Validation
    # Use the system name from the filename regex, or folder? Usually folder is safer if regex fails.
    # We'll use the one from regex 'system' group to be consistent with filename.
    system_name_file = m.group("system")

    ok, msg = validate_contract_exists(system_name_file, table_name)
    if not ok:
        print(f"FAILED: {msg}")
        log_audit_record(bucket_name, file_path, filename, system_name_file, table_name, file_date, "FAILED", msg)
        return

    # 6. Success Logging
    print(f"✅ Success: {filename} valid.")
    log_audit_record(
        bucket_name,
        file_path,
        filename,
        system_name_file,
        table_name,
        file_date,
        "SUCCESS",
        "" # No error
    )
    return