BEGIN TRANSACTION;

  -- 1. Clean up old rules
  DELETE FROM `ops_metadata.standards_definition`
  WHERE standard_id = 'BRONZE_V1';

  -- 2. Insert Pro Rules (Structure + Existence)
  INSERT INTO `ops_metadata.standards_definition`
  (standard_id, layer, column_name, expected_type, is_mandatory, is_partition_col, is_cluster_col)
  VALUES
    -- -------------------------
    -- STRUCTURAL RULES (The Pro Stuff)
    -- -------------------------
    -- Rule 1: MUST Partition by ingestion time (Cost Control)
    ('BRONZE_V1', 'BRONZE', 'meta_ingest_timestamp', 'TIMESTAMP', TRUE, TRUE, FALSE),

    -- Rule 2: MUST Cluster by Unique ID (Point Lookup Perf)
    ('BRONZE_V1', 'BRONZE', 'meta_row_uuid', 'STRING', TRUE, FALSE, TRUE),

    -- -------------------------
    -- METADATA EXISTENCE RULES
    -- -------------------------
    ('BRONZE_V1', 'BRONZE', 'meta_batch_id', 'STRING', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_source_system', 'STRING', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_source_filename', 'STRING', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_source_filedate', 'DATE', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_dq_flag', 'STRING', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_created_by', 'STRING', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_created_date', 'TIMESTAMP', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_updated_by', 'STRING', TRUE, FALSE, FALSE),
    ('BRONZE_V1', 'BRONZE', 'meta_updated_date', 'TIMESTAMP', TRUE, FALSE, FALSE);

COMMIT TRANSACTION;