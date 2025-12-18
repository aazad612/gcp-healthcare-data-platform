-- CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_mapping`
CREATE OR REPLACE TABLE  `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_mapping`
(
    -- 1. Matching Logic (The "Key")
    bucket_name STRING NOT NULL,
    filename_pattern STRING NOT NULL,
    archive_path STRING, 
    protected BOOLEAN, 
    file_type STRING NOT NULL, 
    delimiter STRING,
    validation_row_count INT DEFAULT 10,

    -- 2. Business Metadata
    domain STRING NOT NULL,
    system_name STRING NOT NULL,
    entity STRING NOT NULL,
    
    -- 3. Execution Config (Common)
    contract_gcs_path STRING NOT NULL,
    ingestion_config STRING NOT NULL,

    -- DESTINATION (Split for flexibility)
    target_project_id STRING NOT NULL,     -- e.g. 'cli_syn_np'
    target_dataset_id STRING NOT NULL,     -- e.g. 'bronze_dev'
    target_table_name STRING NOT NULL,     -- e.g. 'encounters'

    -- 4. Cloud Function / Dataflow Path
    dataflow_template_gcs STRING,          -- Nullable (if using Airflow only)
    dataflow_service_account STRING,
    
    -- 5. Airflow Path (Future Proofing)
    validation_dag STRING,                 -- Name of DAG to trigger for validation
    ingestion_dag STRING,                  -- Name of DAG to trigger for ingestion
    airflow_env STRING,                    -- e.g., 'composer-dev-01' or 'airflow-k8s'
    dag_config_json_gcs STRING,                  -- Extra args: {"pool": "sensor_pool", "retries": 3}
    
    -- 6. Traffic Control
    orchestration_mode STRING DEFAULT 'CLOUDFUNCTION', -- 'CLOUDFUNCTION' | 'AIRFLOW' | 'OFF'
    sla_minutes INT64,                     -- Alert if processing takes > X mins

    -- 7. Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


