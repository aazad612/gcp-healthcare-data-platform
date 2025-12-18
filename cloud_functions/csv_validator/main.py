import logging
from context import IngestionContext
from steps import (
    only_process_data_files,
    file_validity_prechecks,
    make_audit_entry,
    call_dataflow
)

from csv_validator import csv_content_validation, csv_schema_drift_checks

import functions_framework

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s:%(funcName)s → %(message)s"
)

logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def trigger_ingestion(cloud_event):
    data = cloud_event.data
    
    # 0. INITIALIZE CONTEXT
    ctx = IngestionContext(
        bucket=data["bucket"],
        file_path=data["name"],
        event_id=cloud_event.id,
        size=int(data.get("size", 0)),
        time_created=data.get("timeCreated")
    )
    
    logger.info(f"START INGESTION: {ctx.ingestion_id} | File: {ctx.file_path}")

    if not only_process_data_files(ctx):
        return "SKIP"

    try:
        if not file_validity_prechecks(ctx):
            make_audit_entry(ctx) # Log failure
            return "STOPPED_PRECHECK"

        if not csv_content_validation(ctx):
            make_audit_entry(ctx) # Log failure
            return "STOPPED_VALIDATION"

        if not csv_schema_drift_checks(ctx):
            make_audit_entry(ctx) # Log failure
            return "STOPPED_SCHEMA"

        ctx.status = "SUCCESS" # Ready to trigger
        make_audit_entry(ctx)

        call_dataflow(ctx)
        
    except Exception as e:
        logger.exception("CRITICAL PIPELINE FAILURE")
        ctx.add_error(str(e))
        make_audit_entry(ctx)
        return "CRITICAL_FAILURE"

    return "OK"


# ... [Paste this at the bottom of main.py] ...

if __name__ == "__main__":
    import os
    import sys
    
    # ---------------------------------------------------------
    # 1. LOCAL SETUP (Mocking Env)
    # ---------------------------------------------------------
    print("\n⚡️ SETTING UP LOCAL TEST ENVIRONMENT...")
    os.environ['OPS_PROJECT'] = 'prj-lbd-shared-np'  # <--- Verify your project ID
    os.environ['CONFIG_BUCKET'] = ''

    # ---------------------------------------------------------
    # 2. MOCKING (Prevents real API calls / costs)
    # ---------------------------------------------------------
    # Mock the CloudEvent class since we aren't triggered by HTTP
    class MockCloudEvent:
        def __init__(self, data):
            self.data = data
            self.id = "local-test-event-uuid"

    # OPTIONAL: Override the Dataflow step to avoid launching real jobs
    # Comment this out if you REALLY want to launch a job from your laptop.
    def mock_call_dataflow(ctx):
        print(f"   [MOCK] 🚀 Dataflow would launch here for: {ctx.table_name}")
        # print(f"   [MOCK] Config: {ctx.bq_config.runtime_config_json}")
    
    # Apply the monkeypatch
    import steps
    steps.call_dataflow = mock_call_dataflow

    # ---------------------------------------------------------
    # 3. TEST CASES
    # ---------------------------------------------------------
    test_cases = [
        # Case 1: HAPPY PATH (Should print SUCCESS)
        {
            "bucket": "bkt-clin-syn-lake-dev-prj-clin-syn-np",
            "name": "incoming/clinical/20251214/synthea/encounters-2025-01-01.csv",
            "size": 1024,
            "timeCreated": "2025-12-14T12:00:00Z"
        },
        # # Case 2: BAD FOLDER DATE (Should fail Pre-checks)
        # {
        #     "bucket": "bkt-clin-syn-lake-dev-prj-clin-syn-np",
        #     "name": "incoming/clinical/2025-12-14/synthea/encounters-2025-01-01.csv",
        #     "size": 1024,
        #     "timeCreated": "2025-12-14T12:00:00Z"
        # },
        # # Case 3: BAD FILENAME DATE (Should fail Pattern Match)
        # {
        #     "bucket": "bkt-clin-syn-lake-dev-prj-clin-syn-np",
        #     "name": "incoming/clinical/20251214/synthea/encounters-20250101.csv",
        #     "size": 1024,
        #     "timeCreated": "2025-12-14T12:00:00Z"
        # }
    ]

    # ---------------------------------------------------------
    # 4. EXECUTION
    # ---------------------------------------------------------
    print("\n🧪 STARTING TESTS...\n")
    
    for i, data in enumerate(test_cases):
        print(f"--- TEST {i+1}: {data['name']} ---")
        
        event = MockCloudEvent(data)
        
        try:
            # Call the main entry point directly
            result = trigger_ingestion(event)
            print(f"👉 RESULT: {result}")
        except Exception as e:
            print(f"💥 CRASHED: {e}")
            import traceback
            traceback.print_exc()
            
        print("\n")