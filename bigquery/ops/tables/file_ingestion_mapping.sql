-- CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_mapping`
CREATE OR REPLACE TABLE  `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_mapping`
(
    env STRING NOT NULL,
    -- 1. Matching Logic (The "Key")
    bucket_name STRING NOT NULL,
    filename_pattern STRING NOT NULL,
    archive_path STRING, 
    protected BOOLEAN, 

    --2. Ingestion Parameters 
    file_type STRING NOT NULL, 
    delimiter STRING,
    validation_row_count INT DEFAULT 10,
    skip_leading_rows INT64 DEFAULT 1,
    allow_jagged_rows BOOLEAN DEFAULT FALSE,

    -- 2. Business Metadata
    domain STRING NOT NULL,
    system_name STRING NOT NULL,
    entity STRING NOT NULL,
    
    -- 3. Execution Config (Common)
    gcs_config_bucket STRING NOT NULL, 
    gcs_json_contract STRING NOT NULL,
    gcs_yaml_ingestion_config STRING NOT NULL,

    -- 4. DESTINATION (Split for flexibility)
    target_project_id STRING NOT NULL,     -- e.g. 'cli_syn_np'
    target_dataset_id STRING NOT NULL,     -- e.g. 'bronze_dev'
    target_table_name STRING NOT NULL,     -- e.g. 'encounters'

    -- 5. Dataflow Path
    gcs_dataflow_template    STRING,          -- Nullable (if using Airflow only)
    dlq_method STRING, -- 'pubsub', 'bq', 'gcs'

    -- 6. Airflow Path (Future Proofing)
    validation_dag STRING,                 -- Name of DAG to trigger for validation
    ingestion_dag STRING,                  -- Name of DAG to trigger for ingestion
    airflow_env STRING,                    -- e.g., 'composer-dev-01' or 'airflow-k8s'
    dag_config_json_gcs STRING,                  -- Extra args: {"pool": "sensor_pool", "retries": 3}
    
    -- 7. Traffic Control
    orchestration_mode STRING DEFAULT 'CLOUDFUNCTION', -- 'CLOUDFUNCTION' | 'AIRFLOW' | 'OFF'
    sla_minutes INT64,                     -- Alert if processing takes > X mins

    -- 8. Audit
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT chk_env CHECK (env IN ('dev', 'qa', 'uat', 'prod'))
);


