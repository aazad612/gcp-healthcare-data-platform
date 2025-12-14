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
    """Central Configuration loaded from Environment Variables"""
    def __init__(self):
        self.PROJECT_ID = os.environ.get("PROJECT_ID")
        self.CONFIG_BUCKET = os.environ.get("CONFIG_BUCKET") 
        self.AUDIT_TABLE = os.environ.get("AUDIT_TABLE")
        self.ALERT_TOPIC = os.environ.get("ALERT_TOPIC") # e.g. projects/x/topics/alerts
        self.DATAFLOW_REGION = os.environ.get("DATAFLOW_REGION", "us-central1")
        self.DATAFLOW_TEMPLATE = os.environ.get("DATAFLOW_TEMPLATE") # gs://...
        self.TEMP_BUCKET = os.environ.get("TEMP_BUCKET") # gs://...
        self.SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT_EMAIL") # dataflow-sa@...

        # Validation: Fail fast if config is missing
        if not all([self.PROJECT_ID, self.CONFIG_BUCKET, self.AUDIT_TABLE]):
            raise ValueError("Critical Environment Variables are missing.")
        
class bq_audit_logger:
    """Handles all interactions with BigQuery Audit Logs"""
    def __init__(self, client: bigquery.Client, table_id: str):
        self.client = client
        self.table_id = table_id

    def bq_log(self, meta: dict, status: str, errors: list = None):
        """
        meta: dict with keys {bucket, object_path, filename, system, entity, file_date}
        """
        row = {
            "ingestion_id": str(uuid.uuid4()),
            "bucket": meta.get("bucket"),
            "object_path": meta.get("object_path"),
            "file_name": meta.get("filename"),
            "entity": meta.get("entity"),
            "system_name": meta.get("system"),
            "arrival_date": meta.get("file_date", datetime.utcnow().date().isoformat()),
            "validation_status": status,
            "validation_errors": errors or [],
            "meta_ingest_timestamp": datetime.utcnow().isoformat()
        }
        
        try:
            insert_errors = self.client.insert_rows_json(self.table_id, [row])
            if insert_errors:
                logger.error(f"BQ Audit Insert Failed: {insert_errors}")
            else:
                logger.info(f"Audit record ({status}) saved.")
        except Exception as e:
            logger.error(f"Critical Audit Failure: {e}")

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