CREATE TABLE IF NOT EXISTS `{{PROJECT_ID}}.{{DATASET_ID}}.standards_definition` (
    standard_id STRING,
    layer STRING,
    column_name STRING,

    -- Level 1: Existence
    is_mandatory BOOL,

    -- Level 2: Strict Types
    expected_type STRING,

    -- Level 3: Description (Documentation enforcement)
    require_description BOOL DEFAULT TRUE,

    -- Level 4: Structure (New!)
    is_partition_col BOOL DEFAULT FALSE,
    is_cluster_col BOOL DEFAULT FALSE,

    active_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    active_to TIMESTAMP
);