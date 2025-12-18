INSERT INTO `{{PROJECT_ID}}.{{DATASET_ID}}.file_ingestion_mapping` (
    -- 1. Matching Logic
    bucket_name, 
    filename_pattern, 
    archive_path, 
    protected,
    file_type, 
    delimiter,

    -- 2. Business Metadata
    domain, 
    system_name, 
    entity,

    -- 3. Execution Config
    contract_gcs_path,
    ingestion_config,

    -- 4. Destination
    target_project_id, 
    target_dataset_id, 
    target_table_name,

    -- 5. Dataflow
    dataflow_template_gcs, 
    dataflow_service_account,

    -- 6. Airflow Path (Future Proofing - NULL for now)
    validation_dag, 
    ingestion_dag, 
    airflow_env, 
    dag_config_json_gcs,

    -- 7. Traffic Control
    orchestration_mode, 
    sla_minutes
)
VALUES (
    -- Matching
    'bkt-clin-syn-lake-dev-prj-clin-syn-np', 
    'incoming/<domain>/<yyyymmdd>/<system_name>/<table_name>-<yyyy-mm-dd>.csv', 
    'archive/<domain>/<yyyymmdd>/<system_name>/<table_name>-<yyyy-mm-dd>.csv', 
    TRUE,
    'csv',
    ',',

    -- Metadata
    'clinical', 
    'synthea', 
    'encounter',

    -- 3. Execution Configs
    'gs://bkt-clin-syn-configs-np/<domain>/<system_name>/bronze/contracts/<table_name>.json',
    'gs://bkt-clin-syn-configs-np/<domain>/<system_name>/bronze/ingestion_configs/<table_name>.yaml',

    -- 4. Destination 
    'cli_syn_np', 
    'bronze_dev', 
    'encounters',

    -- 5. Dataflow 
    'gs://bkt-clin-syn-configs-np/<domain>/<system_name>/bronze/dataflow/<table_name>.json', 
    'sa-dataflow-runner@cli_syn_np.iam.gserviceaccount.com',

    -- 6. Airflow (Unused)
    NULL, NULL, NULL, NULL,

    -- 7. Control
    'CLOUDFUNCTION', 
    60
);