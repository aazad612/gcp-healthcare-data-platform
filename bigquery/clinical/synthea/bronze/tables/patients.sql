CREATE TABLE IF NOT EXISTS patients (
    -- 1. Business Data
    id STRING,
    birthdate DATE,
    deathdate DATE,
    ssn STRING,
    drivers STRING,
    passport STRING,
    prefix STRING,
    first_name STRING,
    last_name STRING,
    suffix STRING,
    maiden STRING,
    marital STRING,
    race STRING,
    ethnicity STRING,
    gender STRING,
    birthplace STRING,
    address STRING,
    city STRING,
    state STRING,
    zip STRING,

    -- 2. Identification Metadata
    meta_pk_hash STRING,
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
CLUSTER BY meta_row_uuid;