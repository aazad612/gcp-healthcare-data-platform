import functions_framework
import json
import re
import os
import uuid
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub_v1
from googleapiclient.discovery import build

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Config:
    """
    Central Config. 
    Separates 'Data Project' (Spoke) from 'Shared Project' (Hub).
    """
    def __init__(self):
        # REQUIRED: Basic Context
        self.ENV = os.environ.get("ENVIRONMENT", "np")  # dev, np, prod
        self.DATA_PROJECT = os.environ.get("DATA_PROJECT_ID")   # e.g., project-clin-syn-np
        self.SHARED_PROJECT = os.environ.get("SHARED_PROJECT_ID") # e.g., shared_np
        
        # REQUIRED: Resources (Constructed or Explicit)
        self.CONFIG_BUCKET = os.environ.get("CONFIG_BUCKET") 
        self.AUDIT_TABLE_ID = os.environ.get("AUDIT_TABLE_ID") # Full ID: project.dataset.table
        
        # REQUIRED: Dataflow
        self.DATAFLOW_TEMPLATE_GCS = os.environ.get("DATAFLOW_TEMPLATE_GCS")
        self.DATAFLOW_TEMP_BUCKET = os.environ.get("DATAFLOW_TEMP_BUCKET") 
        self.DATAFLOW_SA = os.environ.get("DATAFLOW_SERVICE_ACCOUNT")
        self.DATAFLOW_REGION = os.environ.get("DATAFLOW_REGION", "us-central1")
        
        # OPTIONAL: Alerting
        self.ALERT_TOPIC_ID = os.environ.get("ALERT_TOPIC_ID") # Full ID

        # Validation
        if not all([self.DATA_PROJECT, self.SHARED_PROJECT, self.CONFIG_BUCKET, self.AUDIT_TABLE_ID]):
            raise ValueError("CRITICAL: Missing required environment variables.")
        
class BQAuditLogger:
    """
    Handles logging to BigQuery. 
    Renamed methods/vars to avoid collision with data ingestion logic.
    """
    def __init__(self, bq_client: bigquery.Client, table_id: str):
        self.client = bq_client
        self.audit_table = table_id

    def write_audit_entry(self, meta: dict, status: str, errors: list = None, job_id: str = None):
        """
        Inserts a single row into the Audit Table.
        """
        row = {
            "ingestion_id": meta.get("ingestion_id"),
            "bucket": meta.get("bucket"),
            "object_path": meta.get("object_path"),
            "file_name": meta.get("file_name"),
            
            "domain": meta.get("domain", "UNKNOWN"),
            "system_name": meta.get("system_name", "UNKNOWN"),

            "file_size_bytes": meta.get("file_size_bytes"),
            "file_type": meta.get("file_type"),
            
            # Dates
            "file_arrival_time": meta.get("arrival_ts"), # GCS creation time
            "ingestion_started": datetime.utcnow().isoformat(),
            "file_date": meta.get("file_date"), # YYYY-MM-DD
            
            # Status
            "validation_status": status,
            "validation_errors": errors or [],
            "dataflow_job_id": job_id,
            
            # Routing (Default to incoming for now)
            "routed_to": "",
            "routed_path": f"gs://{meta.get('bucket')}/{meta.get('object_path')}"
        }
        
        try:
            # We use ignore_unknown_values=True to be safe against schema updates
            insert_errors = self.client.insert_rows_json(
                self.audit_table, [row], ignore_unknown_values=True
            )
            if insert_errors:
                logger.error(f"AUDIT INSERT FAILED: {insert_errors}")
            else:
                logger.info(f"Audit record saved: {status}")
        except Exception as e:
            logger.error(f"CRITICAL AUDIT EXCEPTION: {e}")

class IngestionManager:
    """Core Logic for Validation and Orchestration"""
    
    # Regex for filename parsing
    FILENAME_PATTERN = re.compile(
        r"^(?P<table>[a-zA-Z0-9]+)_(?P<system>[a-zA-Z0-9]+)-(?P<date>\d{8})\.(csv|json)$"
    )

    def __init__(self):
        self.cfg = Config()
        self.storage = storage.Client()
        self.bq = bigquery.Client()
        self.pubsub = pubsub_v1.PublisherClient()
        self.auditor = AuditLogger(self.bq, self.cfg.AUDIT_TABLE)

    def get_blob_content(self, bucket_name, blob_name):
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists(): return None
        return blob.download_as_text()

    def send_alert(self, message):
        """Sends notification to Pub/Sub for immediate attention"""
        if not self.cfg.ALERT_TOPIC: return
        try:
            self.pubsub.publish(self.cfg.ALERT_TOPIC, message.encode("utf-8"))
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

    def validate_and_process(self, bucket_name, file_path):
        filename = file_path.split("/")[-1]
        
        # 1. PARSE METADATA
        m = self.FILENAME_PATTERN.match(filename)
        if not m:
            error = f"Invalid Filename Format: {filename}"
            self.auditor.log({"bucket": bucket_name, "object_path": file_path, "filename": filename}, "FAILED", [error])
            self.send_alert(f"Ingestion Blocked: {error}")
            return

        meta = {
            "bucket": bucket_name,
            "object_path": file_path,
            "filename": filename,
            "entity": m.group("table"),
            "system": m.group("system"),
            "file_date": datetime.strptime(m.group("date"), "%Y%m%d").date().isoformat()
        }

        # 2. VALIDATE CONTRACT EXISTENCE
        contract_path = f"contracts/{meta['entity'].lower()}_v1.json"
        contract_json = self.get_blob_content(self.cfg.CONFIG_BUCKET, contract_path)
        
        if not contract_json:
            error = f"Contract Missing: {contract_path}"
            self.auditor.log(meta, "FAILED", [error])
            self.send_alert(f"Ingestion Blocked: {error}")
            return

        # 3. SCHEMA DRIFT CHECK
        try:
            expected_schema = json.loads(contract_json)
            # Read first line of CSV for header
            csv_head = self.get_blob_content(bucket_name, file_path).splitlines()[0]
            actual_cols = [c.strip() for c in csv_head.split(",")]
            
            expected_cols = [f['name'] for f in expected_schema if not f['name'].startswith('meta_')]
            
            # Simple Set Logic
            missing = set(expected_cols) - set(actual_cols)
            # extra = set(actual_cols) - set(expected_cols) # Uncomment if you want strict equality

            if missing:
                error = f"Schema Drift Detected. Missing columns: {list(missing)}"
                self.auditor.log(meta, "FAILED", [error])
                self.send_alert(f"Ingestion Blocked: {error}")
                return

        except Exception as e:
            error = f"Schema Validation Exception: {str(e)}"
            self.auditor.log(meta, "FAILED", [error])
            return

        # 4. SUCCESS - LAUNCH JOB
        self.auditor.log(meta, "SUCCESS")
        self.launch_dataflow(meta)

    def launch_dataflow(self, meta):
        """Launches the Dataflow Template"""
        logger.info(f"Launching Dataflow for {meta['filename']}...")
        
        service = build('dataflow', 'v1b3', cache_discovery=False)
        job_name = f"ingest-{meta['entity']}-{uuid.uuid4().hex[:8]}"

        params = {
            "input_file": f"gs://{meta['bucket']}/{meta['object_path']}",
            "table_name": meta['entity'], # Used by pipeline to find parser
            "batch_id": str(uuid.uuid4())
        }

        request = service.projects().templates().launch(
            projectId=self.cfg.PROJECT_ID,
            location=self.cfg.DATAFLOW_REGION,
            gcsPath=self.cfg.DATAFLOW_TEMPLATE,
            body={
                "jobName": job_name,
                "parameters": params,
                "environment": {
                    "tempLocation": f"{self.cfg.TEMP_BUCKET}/temp",
                    "serviceAccountEmail": self.cfg.SERVICE_ACCOUNT
                }
            }
        )
        try:
            request.execute()
            logger.info(f"Dataflow Job {job_name} started.")
        except Exception as e:
            logger.error(f"Failed to launch Dataflow: {e}")
            self.send_alert(f"Dataflow Launch Failed for {meta['filename']}")


# Global Instance to reuse clients across invocations (Warm Start)
manager = IngestionManager()

@functions_framework.cloud_event
def ingest_validator(cloud_event):
    """Entry Point"""
    data = cloud_event.data
    
    # Handle GCS Event format
    bucket = data.get("bucket")
    name = data.get("name")
    
    if not bucket or not name:
        logger.error("Invalid Event Data")
        return

    # Skip temp/staging folders
    if "temp/" in name or "staging/" in name:
        return

    manager.validate_and_process(bucket, name)