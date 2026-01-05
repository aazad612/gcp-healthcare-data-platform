# dataflow/common/metadata_loader.py

import yaml
from google.cloud import bigquery
from google.cloud import storage
import datetime


def parse_filename(file_name: str) -> dict:
    """
    Expected pattern:
    incoming/<domain>/<yyyymmdd>/<system>/<entity>-<yyyymmdd>.<ext>
    """

    parts = file_name.split("/")
    domain = parts[1]
    arrival_date = parts[2]
    system = parts[3]
    entity = parts[4].split("-")[0]
    file_date = parts[4].split("-")[1].split(".")[0]

    return {
        "domain": domain,
        "system": system,
        "entity": entity,
        "arrival_date": arrival_date,
        "file_date": file_date,
        "file_name": file_name
    }


def load_ingestion_mapping(bq_client, parsed):
    query = f"""
        SELECT *
        FROM ops_metadata.file_ingestion_mapping
        WHERE is_active = TRUE
          AND domain = @domain
          AND system_name = @system
          AND entity = @entity
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("domain", "STRING", parsed["domain"]),
            bigquery.ScalarQueryParameter("system", "STRING", parsed["system"]),
            bigquery.ScalarQueryParameter("entity", "STRING", parsed["entity"]),
        ]
    )

    rows = list(bq_client.query(query, job_config=job_config).result())
    if not rows:
        raise RuntimeError("No active ingestion mapping found")

    return dict(rows[0])


def load_yaml_contract(gcs_client, bucket, path):
    blob = gcs_client.bucket(bucket).blob(path)
    return yaml.safe_load(blob.download_as_text())


def load_standards(bq_client, standard_id, layer):
    query = f"""
        SELECT column_name, expected_type
        FROM ops_metadata.standards_definition
        WHERE standard_id = @sid
          AND layer = @layer
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("sid", "STRING", standard_id),
            bigquery.ScalarQueryParameter("layer", "STRING", layer),
        ]
    )

    rows = bq_client.query(query, job_config=job_config).result()
    return {r.column_name: r.expected_type for r in rows}


def load_runtime_metadata(file_name: str):
    parsed = parse_filename(file_name)

    bq = bigquery.Client()
    gcs = storage.Client()

    mapping = load_ingestion_mapping(bq, parsed)

    yaml_contract = load_yaml_contract(
        gcs,
        mapping["gcs_config_bucket"],
        mapping["gcs_yaml_ingestion_config"],
    )

    dlq_method = mapping["dlq_method"].lower()
    use_storage_api = "pubsub" in dlq_method

    # ---- job-level metadata (computed ONCE per run) ----
    job_ts = datetime.now(timezone.utc)

    batch_query = f"""
        SELECT COALESCE(MAX(meta_batch_id), '0') AS max_batch_id
        FROM `{mapping["target_project_id"]}.{mapping["target_dataset_id"]}.{mapping["target_table_name"]}`
    """
    rows = list(bq.query(batch_query).result())
    next_batch_id = str(int(rows[0]["max_batch_id"]) + 1)

    return {
        # identity (from filename)
        **parsed,

        # routing
        "env": mapping["env"],
        "target_project": mapping["target_project_id"],
        "target_dataset": mapping["target_dataset_id"],
        "target_table": mapping["target_table_name"],

        # contracts
        "yaml_contract": yaml_contract,
        "pk_columns": yaml_contract["schema"].get("primary_key"),
        "schema_version": yaml_contract.get("version"),

        # DLQ / execution
        "dlq_method": dlq_method,
        "dlq_topic": mapping.get("dlq_topic"),
        "use_storage_write_api": use_storage_api,

        # storage
        "bucket_name": mapping["bucket_name"],
        "archive_path": mapping["archive_path"],

        # ---- job-level meta columns ----
        "meta_batch_id": next_batch_id,
        "meta_ingest_timestamp": job_ts,
        "meta_source_system": parsed["system"],
        "meta_source_filename": parsed["file_name"],
        "meta_source_filedate": parsed["file_date"],
        "meta_created_by": "dataflow",
        "meta_created_date": job_ts,
        "meta_updated_by": "dataflow",
        "meta_updated_date": job_ts,
    }
