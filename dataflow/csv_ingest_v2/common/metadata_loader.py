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


from google.cloud import bigquery


def load_ingestion_mapping(bq_client, parsed, dataflow_project: str):
    """
    Load active ingestion mapping from ops_metadata.file_ingestion_mapping.

    Uses ops project = dataflow_project (authoritative).
    """

    table = f"`{dataflow_project}.ops_metadata.file_ingestion_mapping`"

    query = f"""
        SELECT *
        FROM {table}
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
        raise RuntimeError(
            "No active ingestion mapping found for "
            f"domain={parsed['domain']}, "
            f"system={parsed['system']}, "
            f"entity={parsed['entity']} "
            f"in {table}"
        )

    return dict(rows[0])



def load_yaml_contract(gcs_client, bucket, path):
    blob = gcs_client.bucket(bucket).blob(path)
    return yaml.safe_load(blob.download_as_text())


from google.cloud import bigquery


def load_standards(
    bq_client,
    *,
    dataflow_project: str,
    standard_id: str,
    layer: str,
) -> dict:
    """
    Load standards definition for a given standard_id and layer.

    Returns a dict keyed by column_name with full rule metadata.

    Standards are authoritative for:
      - meta columns
      - required columns
      - partition / clustering
    """

    table = f"`{dataflow_project}.ops_metadata.standards_definition`"

    query = f"""
        SELECT
            column_name,
            expected_type,
            is_mandatory,
            is_partition_col,
            is_cluster_col
        FROM {table}
        WHERE standard_id = @standard_id
          AND layer = @layer
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "standard_id", "STRING", standard_id
            ),
            bigquery.ScalarQueryParameter(
                "layer", "STRING", layer
            ),
        ]
    )

    rows = list(bq_client.query(query, job_config=job_config).result())

    if not rows:
        raise RuntimeError(
            f"No standards found for standard_id={standard_id}, layer={layer} "
            f"in {table}"
        )

    standards = {}
    for r in rows:
        standards[r.column_name] = {
            "expected_type": r.expected_type,
            "is_mandatory": bool(r.is_mandatory),
            "is_partition_col": bool(r.is_partition_col),
            "is_cluster_col": bool(r.is_cluster_col),
        }

    return standards


def merge_contract_and_standards(
    *,
    contract: dict,
    standards: dict,
    schema_drift_policy: dict,
) -> dict:
    """
    Merge business contract schema with platform standards.

    Rules:
      - Contract defines business columns only
      - Standards inject meta / platform columns
      - Standards are authoritative for meta column types
      - Contract type mismatches are governed by schema_drift_policy
      - Partition / clustering come ONLY from standards

    Returns:
      {
        schema_columns: { col -> {type, mode, format?, description?} },
        partition_cols: [ ... ],
        cluster_cols: [ ... ]
      }
    """

    # ---------------------------------------------------------
    # 1. Build base schema from contract (business columns)
    # ---------------------------------------------------------
    schema_columns = {}

    for col in contract["schema"]["columns"]:
        name = col["name"]

        schema_columns[name] = {
            "type": col["type"],
            "mode": col.get("mode", "NULLABLE"),
            "format": col.get("format"),
            "description": col.get("description"),
            "source": "contract",
        }

    # ---------------------------------------------------------
    # 2. Apply standards (inject + enforce)
    # ---------------------------------------------------------
    partition_cols = []
    cluster_cols = []

    allow_type_widening = schema_drift_policy.get(
        "allow_type_widening", False
    )

    for col_name, rule in standards.items():
        std_type = rule["expected_type"]

        if col_name in schema_columns:
            # Column exists in contract → enforce policy
            contract_type = schema_columns[col_name]["type"]

            if contract_type.upper() != std_type.upper():
                if not allow_type_widening:
                    raise RuntimeError(
                        f"Type mismatch for column '{col_name}': "
                        f"contract={contract_type}, standard={std_type} "
                        f"and schema_drift_policy forbids widening"
                    )

                # allowed widening → enforce standard
                schema_columns[col_name]["type"] = std_type

        else:
            # Inject meta / platform column
            schema_columns[col_name] = {
                "type": std_type,
                "mode": "REQUIRED" if rule["is_mandatory"] else "NULLABLE",
                "format": None,
                "description": "injected by platform standards",
                "source": "standard",
            }

        # Structural rules
        if rule.get("is_partition_col"):
            partition_cols.append(col_name)

        if rule.get("is_cluster_col"):
            cluster_cols.append(col_name)

    # ---------------------------------------------------------
    # 3. Enforce additive column policy (optional but correct)
    # ---------------------------------------------------------
    if not schema_drift_policy.get("allow_additive_columns", False):
        # standards + contract columns are the only allowed columns
        # (actual file-level enforcement happens later)
        pass

    return {
        "schema_columns": schema_columns,
        "partition_cols": partition_cols,
        "cluster_cols": cluster_cols,
    }


def build_dlq_schema_from_standards(
    *,
    dlq_standards: dict,
) -> dict:
    """
    Build DLQ schema purely from DLQ standards.

    Rules:
      - DLQ schema is fully governed by standards_definition (DLQ_V1)
      - No contract involved
      - No schema drift allowed
      - Partition / clustering derived from standards only

    Returns:
      {
        dlq_schema: { col -> {type, mode} },
        partition_cols: [ ... ],
        cluster_cols: [ ... ]
      }
    """

    dlq_schema = {}
    partition_cols = []
    cluster_cols = []

    for col_name, rule in dlq_standards.items():
        dlq_schema[col_name] = {
            "type": rule["expected_type"],
            "mode": "REQUIRED" if rule["is_mandatory"] else "NULLABLE",
        }

        if rule.get("is_partition_col"):
            partition_cols.append(col_name)

        if rule.get("is_cluster_col"):
            cluster_cols.append(col_name)

    return {
        "dlq_schema": dlq_schema,
        "partition_cols": partition_cols,
        "cluster_cols": cluster_cols,
    }


from datetime import datetime, timezone
import uuid
import yaml
from google.cloud import bigquery, storage


def build_runtime_metadata(
    *,
    gcs_uri: str,
    org_vars_path: str = "org_df_vars.yaml",
) -> dict:
    """
    Authoritative control-plane builder.

    Order of resolution:
      1. parse_gcs_uri
      2. resolve_org_df_vars
      3. load_ingestion_mapping
      4. load_yaml_contract
      5. load standards (BRONZE + DLQ)
      6. merge schemas
    """

    # ------------------------------------------------------------
    # 1. Parse URI (env, domain, system, entity)
    # ------------------------------------------------------------
    parsed = parse_gcs_uri(gcs_uri)

    # ------------------------------------------------------------
    # 2. Resolve infra from org_df_vars.yaml (ROOT OF TRUTH)
    # ------------------------------------------------------------
    infra = resolve_org_df_vars(
        env=parsed["env"],
        domain=parsed["domain"],
        system=parsed["system"],
        org_vars_path=org_vars_path,
    )

    dataflow_project = infra["dataflow_project"]

    # ------------------------------------------------------------
    # 3. Control-plane clients (explicit project)
    # ------------------------------------------------------------
    bq_client = bigquery.Client(project=dataflow_project)
    gcs_client = storage.Client(project=dataflow_project)

    # ------------------------------------------------------------
    # 4. Ingestion mapping (routing + standards + contract pointer)
    # ------------------------------------------------------------
    ingestion_mapping = load_ingestion_mapping(
        bq_client=bq_client,
        parsed=parsed,
        dataflow_project=dataflow_project,
    )

    # Required fields in ingestion_mapping
    target_table = ingestion_mapping["target_table"]
    contract_path = ingestion_mapping["contract_path"]
    standard_id = ingestion_mapping.get("standard_id", "BRONZE_V1")
    dlq_standard_id = ingestion_mapping.get("dlq_standard_id", "DLQ_V1")
    dlq_method = ingestion_mapping.get("dlq_method", [])
    dlq_topic = ingestion_mapping.get("dlq_topic")

    # ------------------------------------------------------------
    # 5. Load contract (business schema only)
    # ------------------------------------------------------------
    contract = load_yaml_contract(
        gcs_client,
        bucket=infra["config_bucket"],
        path=contract_path,
    )

    schema_drift_policy = contract.get("schema_drift_policy", {
        "allow_additive_columns": False,
        "allow_type_widening": False,
        "on_violation": "REJECT",
    })

    # ------------------------------------------------------------
    # 6. Load standards (meta / structure)
    # ------------------------------------------------------------
    bronze_standards = load_standards(
        bq_client=bq_client,
        dataflow_project=dataflow_project,
        standard_id=standard_id,
        layer="BRONZE",
    )

    dlq_standards = load_standards(
        bq_client=bq_client,
        dataflow_project=dataflow_project,
        standard_id=dlq_standard_id,
        layer="DLQ",
    )

    # ------------------------------------------------------------
    # 7. Merge schemas
    # ------------------------------------------------------------
    merged_schema = merge_contract_and_standards(
        contract=contract,
        standards=bronze_standards,
        schema_drift_policy=schema_drift_policy,
    )

    dlq_schema = build_dlq_schema_from_standards(
        dlq_standards=dlq_standards
    )

    # ------------------------------------------------------------
    # 8. Job / batch metadata
    # ------------------------------------------------------------
    batch_id = str(uuid.uuid4())
    ingest_ts = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------
    # 9. Final runtime_metadata (single truth passed to pipeline)
    # ------------------------------------------------------------
    runtime_metadata = {
        # ---------------- identity ----------------
        "gcs_uri": gcs_uri,
        "bucket": parsed["bucket"],
        "env": parsed["env"],
        "domain": parsed["domain"],
        "system": parsed["system"],
        "entity": parsed["entity"],
        "arrival_date": parsed["arrival_date"],
        "file_date": parsed["file_date"],
        "ext": parsed["ext"],

        # ---------------- execution infra ----------------
        "region": infra["region"],
        "use_public_ips": infra["use_public_ips"],
        "dataflow_project": dataflow_project,
        "dataflow_sa": infra["dataflow_sa"],
        "temp_bucket": infra["temp_bucket"],
        "staging_bucket": infra["staging_bucket"],
        "subnetwork": infra["subnetwork"],

        # ---------------- target routing ----------------
        "target_project": infra["target_project"],
        "target_dataset": infra["target_dataset"],
        "target_table": target_table,

        # ---------------- schema (bronze) ----------------
        "schema_columns": merged_schema["schema_columns"],
        "partition_cols": merged_schema["partition_cols"],
        "cluster_cols": merged_schema["cluster_cols"],
        "pk_columns": contract["schema"]["primary_key"],
        "schema_version": contract.get("version"),

        # ---------------- DLQ ----------------
        "dlq_method": dlq_method,
        "dlq_topic": dlq_topic,
        "dlq_gcs_bucket": infra["landing_bucket"],
        "dlq_schema": dlq_schema["dlq_schema"],
        "dlq_partition_cols": dlq_schema["partition_cols"],
        "dlq_cluster_cols": dlq_schema["cluster_cols"],

        # ---------------- job metadata ----------------
        "batch_id": batch_id,
        "meta_batch_id": batch_id,
        "meta_ingest_timestamp": ingest_ts,
    }

    return runtime_metadata


if __name__ == "__main__":
    """
    Local sanity test for control-plane resolution only.

    This:
      - parses GCS URI
      - resolves org_df_vars.yaml
      - loads ingestion mapping
      - loads contract
      - loads standards
      - builds runtime_metadata

    It DOES NOT:
      - run Beam
      - touch Dataflow
      - write to BigQuery
      - write to GCS
    """

    # Example input — replace with a real one
    gcs_uri = (
        "gs://bkt-clin-syn-lake-dev-prj-clin-syn-np/"
        "incoming/clin/20240101/synthea/encounters_20240101.csv"
    )

    runtime_metadata = build_runtime_metadata(
        gcs_uri=gcs_uri,
        org_vars_path="org_df_vars.yaml",
    )

    # Pretty-print for inspection
    import json
    print(json.dumps(runtime_metadata, indent=2, default=str))
