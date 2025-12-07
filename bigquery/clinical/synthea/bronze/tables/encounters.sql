CREATE TABLE IF NOT EXISTS encounters (
    id STRING,
    start STRING,
    stop STRING,
    patient STRING,
    organization STRING,
    provider STRING,
    encounterclass STRING,
    code STRING,
    description STRING,
    base_encounter_cost STRING,
    total_claim_cost STRING,
    payer STRING,

    -- 2. Identification Metadata
    meta_row_uuid STRING,
    meta_batch_id STRING,

    -- 3. Source Context
    meta_source_system STRING,
    meta_source_filename STRING,
    meta_source_filedate DATE,

    -- 4. Ingestion Audit
    meta_ingest_timestamp TIMESTAMP,
    meta_dq_flag STRING,

    -- 5. Record Audit
    meta_created_by STRING,
    meta_created_date TIMESTAMP,
    meta_updated_by STRING,
    meta_updated_date TIMESTAMP
)
PARTITION BY DATE(meta_ingest_timestamp)
CLUSTER BY id, meta_source_filedate;
