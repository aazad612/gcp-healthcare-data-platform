DELETE FROM `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_mapping`
WHERE env = 'dev' 
  AND domain = 'clinical' 
  AND system_name = 'synthea' 
  AND entity = 'conditions';

INSERT INTO `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_mapping` (
    env,
    -- 1. Matching Logic
    bucket_name, 
    filename_pattern, 
    archive_path, 
    protected,

    -- 2. Ingestion Parameters
    file_type, 
    delimiter,
    validation_row_count,
    skip_leading_rows,
    allow_jagged_rows,

    -- 3. Business Metadata
    domain, 
    system_name, 
    entity,

    -- 4. Execution Config
    gcs_config_bucket,
    gcs_json_contract,
    gcs_yaml_ingestion_config,

    -- 5. Destination
    target_project_id, 
    target_dataset_id, 
    target_table_name,

    -- 6. Dataflow Path
    gcs_dataflow_template,
    dlq_method,
    dlq_topic,

    -- 7. Airflow Path
    validation_dag, 
    ingestion_dag, 
    airflow_env, 
    dag_config_json_gcs,

    -- 8. Traffic Control
    orchestration_mode, 
    sla_minutes,

    -- 9. Audit
    is_active
)
VALUES (
    'dev',
    -- 1. Matching
    'bkt-clin-syn-lake-dev-prj-clin-syn-np', 
    'incoming/<domain>/<yyyymmdd>/<system_name>/<table_name>-<yyyy-mm-dd>.csv', 
    'archive/<domain>/<yyyymmdd>/<system_name>/<table_name>-<yyyy-mm-dd>.csv', 
    TRUE,

    -- 2. Ingestion Parameters
    'CSV',
    ',',
    10,
    1,
    FALSE,

    -- 3. Metadata
    'clinical', 
    'synthea', 
    'conditions',

    -- 4. Execution Configs
    'bkt-clin-syn-configs-np', 
    'clinical/synthea/bronze/contracts/conditions_v1.json',
    'clinical/synthea/bronze/ingestion_configs/conditions_v1.yaml',

    -- 5. Destination 
    'prj-clin-syn-np', 
    'synthea_bronze_dev', 
    'conditions',
    '',

    -- 6. Dataflow 
    'clinical/synthea/bronze/dataflow/conditions_v1.json',
    'gcs,bq',

    -- 7. Airflow
    NULL, NULL, NULL, NULL,

    -- 8. Control
    'CLOUDFUNCTION', 
    60,
    
    -- 9. Audit
    TRUE
);