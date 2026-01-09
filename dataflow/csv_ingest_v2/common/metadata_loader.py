import yaml
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timezone
import re
import uuid

_GCS_URI_PATTERN = re.compile(
    r"^gs://"
    r"(?P<bucket>[a-z0-9\-]+)"
    r"/incoming/"
    r"(?P<domain>[a-z0-9_]+)/"
    r"(?P<arrival_date>\d{8})/"
    r"(?P<system>[a-z0-9_]+)/"
    r"(?P<entity>[a-z0-9_]+)_"
    r"(?P<file_date>\d{8})"
    r"\.(?P<ext>[a-z0-9]+)$"
)

def parse_gcs_uri(gcs_uri: str) -> dict:
    """
    Strict parser.

    Expected pattern:
      gs://<bucket>/incoming/<domain>/<yyyymmdd>/<system>/<entity>_<yyyymmdd>.<ext>

    Bucket standard:
      bkt-<domain>-<unit>-lake-<env>-...

    Env is extracted from bucket position 5 (index 4).
    """

    m = _GCS_URI_PATTERN.match(gcs_uri)
    if not m:
        raise ValueError(
            "Invalid GCS URI.\n"
            "Expected:\n"
            "  gs://<bucket>/incoming/<domain>/<yyyymmdd>/<system>/<entity>_<yyyymmdd>.<ext>\n"
            f"Got:\n  {gcs_uri}"
        )

    parsed = m.groupdict()
    bucket = parsed["bucket"]

    # ---- extract env from bucket (fixed position) ----
    bucket_parts = bucket.split("-")
    if len(bucket_parts) < 5:
        raise ValueError(
            f"Invalid bucket name '{bucket}': expected env at position 5"
        )

    env = bucket_parts[4]
    if env not in {"dev", "qa", "uat", "pd"}:
        raise ValueError(
            f"Invalid env '{env}' in bucket '{bucket}': "
            "expected one of dev|qa|uat|pd at position 5"
        )

    # ---- final payload ----
    return {
        "gcs_uri": gcs_uri,
        "bucket": bucket,
        "env": env,
        "domain": parsed["domain"],
        "system": parsed["system"],
        "entity": parsed["entity"],
        "arrival_date": parsed["arrival_date"],
        "file_date": parsed["file_date"],
        "ext": parsed["ext"],
    }


def resolve_org_df_vars(
    *,
    env: str,
    domain: str,
    system: str,
    org_vars_path: str = "org_df_vars.yaml",
) -> dict:
    """
    Resolve execution + unit infra from org_df_vars.yaml.

    Inputs:
      env     : dev | qa | uat | pd
      domain  : logical domain (e.g. clinical, research)
      system  : unit/system within domain (e.g. synthea, hospitals)

    Returns:
      dict with shared Dataflow execution infra + unit-level targets.
    """

    with open(org_vars_path, "r") as f:
        cfg = yaml.safe_load(f)

    if "domains" not in cfg:
        raise ValueError("org_df_vars.yaml missing top-level 'domains' key")

    if domain not in cfg["domains"]:
        raise ValueError(f"Domain '{domain}' not found in org_df_vars.yaml")

    domain_cfg = cfg["domains"][domain]

    # ---- shared infra (Dataflow execution context) ----
    shared = domain_cfg.get("shared_infra")
    if not shared:
        raise ValueError(f"Domain '{domain}' missing shared_infra block")

    def _env_lookup(block: dict, name: str):
        if name not in block:
            raise ValueError(f"Missing '{name}' in shared_infra for domain '{domain}'")
        if env not in block[name]:
            raise ValueError(
                f"Env '{env}' not defined for '{name}' in domain '{domain}'"
            )
        return block[name][env]

    execution = {
        "env": env,
        "domain": domain,
        "region": domain_cfg.get("region"),
        "use_public_ips": domain_cfg.get("use_public_ips", False),

        # Dataflow execution infra
        "dataflow_project": _env_lookup(shared, "project"),
        "dataflow_sa": _env_lookup(shared, "dataflow_sa"),
        "temp_bucket": _env_lookup(shared, "temp_bucket"),
        "staging_bucket": _env_lookup(shared, "staging_bucket"),
        "subnetwork": _env_lookup(shared, "subnet"),
    }

    # ---- unit / system infra ----
    units = domain_cfg.get("units")
    if not units:
        raise ValueError(f"Domain '{domain}' has no units defined")

    if system not in units:
        raise ValueError(
            f"System '{system}' not found under domain '{domain}' units"
        )

    unit_cfg = units[system]

    def _unit_env_lookup(block: dict, name: str):
        if name not in block:
            raise ValueError(
                f"Missing '{name}' for system '{system}' in domain '{domain}'"
            )
        if env not in block[name]:
            raise ValueError(
                f"Env '{env}' not defined for '{name}' in system '{system}'"
            )
        return block[name][env]

    unit = {
        "target_project": _unit_env_lookup(unit_cfg, "project"),
        "target_dataset": _unit_env_lookup(unit_cfg, "dataset"),
        "config_bucket": _unit_env_lookup(unit_cfg, "config_bucket"),
        "landing_bucket": _unit_env_lookup(unit_cfg, "landing_bucket"),
    }

    # ---- final resolved metadata ----
    return {
        **execution,
        **unit,
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
