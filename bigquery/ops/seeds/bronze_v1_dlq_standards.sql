BEGIN TRANSACTION;

-- Remove old DLQ standard if re-running
DELETE FROM `ops_metadata.standards_definition`
WHERE standard_id = 'DLQ_V1'
  AND layer = 'DLQ';

-- Insert DLQ standard (governed failure schema)
INSERT INTO `ops_metadata.standards_definition`
(standard_id, layer, column_name, expected_type, is_mandatory, is_partition_col, is_cluster_col)
VALUES
  -- Core identity
  ('DLQ_V1', 'DLQ', 'meta_batch_id',        'STRING',    TRUE,  FALSE, FALSE),
  ('DLQ_V1', 'DLQ', 'meta_pk_hash',         'STRING',    FALSE, FALSE, FALSE),

  -- Failure classification
  ('DLQ_V1', 'DLQ', 'error_stage',          'STRING',    TRUE,  FALSE, FALSE), -- CF / DF / BQ
  ('DLQ_V1', 'DLQ', 'error_category',       'STRING',    TRUE,  FALSE, FALSE), -- SCHEMA / PK / TYPE / SYSTEM
  ('DLQ_V1', 'DLQ', 'error_code',           'STRING',    TRUE,  FALSE, FALSE),
  ('DLQ_V1', 'DLQ', 'error_message',        'STRING',    TRUE,  FALSE, FALSE),

  -- Payload & context
  ('DLQ_V1', 'DLQ', 'payload_type',         'STRING',    TRUE,  FALSE, FALSE), -- ROW / FILE / JOB
  ('DLQ_V1', 'DLQ', 'payload',              'STRING',    TRUE,  FALSE, FALSE),
  ('DLQ_V1', 'DLQ', 'schema_version',       'STRING',    TRUE,  FALSE, FALSE),

  -- Audit
  ('DLQ_V1', 'DLQ', 'meta_ingest_timestamp','TIMESTAMP', TRUE,  TRUE,  FALSE),
  ('DLQ_V1', 'DLQ', 'meta_source_system',   'STRING',    TRUE,  FALSE, FALSE),
  ('DLQ_V1', 'DLQ', 'meta_source_filename', 'STRING',    TRUE,  FALSE, FALSE);

COMMIT TRANSACTION;
