CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_audit`
(
    -- Identifiers
    ingestion_id STRING NOT NULL,                  -- UUID per event
    bucket STRING NOT NULL,
    object_path STRING NOT NULL,                   -- full GCS path
    file_name STRING NOT NULL,                     -- just the filename
    domain STRING NOT NULL,                        -- clinical/enterprise/research
    system_name STRING NOT NULL,                   -- synthea, epic, cerner, etc.

    -- Dates
    file_arrival_time TIMESTAMP NOT NULL,          -- folder-based arrival date
    ingestion_started TIMESTAMP NOT NULL,          -- When Cloud Function inserted this row
    ingestion_completed TIMESTAMP,                 -- Updated by Dataflow when done
    file_date DATE,                                -- YYYYMMDD extracted from filename

    -- File Metadata
    file_size_bytes INT64,
    file_type STRING,                              -- csv, json, parquet, etc.

    -- Validation Results
    validation_status STRING NOT NULL,             -- SUCCESS | FAILED
    validation_errors ARRAY<STRING>,               -- list of errors if failed

    -- Routing Info
    routed_to STRING,                              -- incoming | validated | rejected
    routed_path STRING,                            -- GCS final location

    -- Orchestration Metadata
    pubsub_message_id STRING,
    dataflow_job_id STRING,                        -- ID of the job that processes this file

    -- Standard metadata
    meta_ingest_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(meta_ingest_timestamp)
CLUSTER BY domain, system_name;                  