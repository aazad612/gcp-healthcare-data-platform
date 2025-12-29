"""
metadata_loader.py

Loads runtime metadata for Dataflow using YAML contracts.
"""

from datetime import datetime, timezone
from google.cloud import bigquery, storage
from google.auth import impersonated_credentials
import google.auth
import yaml


def get_bq_client(project_id: str, target_sa: str):
    source_creds, _ = google.auth.default()
    creds = impersonated_credentials.Credentials(
        source_credentials=source_creds,
        target_principal=target_sa,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=3600,
    )
    return bigquery.Client(project=project_id, credentials=creds)


def load_ingestion_mapping(bq, ops_project, ops_dataset, env, domain, system, entity):
    table = f"{ops_project}.{ops_dataset}.file_ingestion_mapping"

    query = f"""
        SELECT *
        FROM `{table}`
        WHERE env=@env AND domain=@domain
          AND system_name=@system AND entity=@entity
          AND is_active=TRUE
        LIMIT 1
    """

    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("env", "STRING", env),
                bigquery.ScalarQueryParameter("domain", "STRING", domain),
                bigquery.ScalarQueryParameter("system", "STRING", system),
                bigquery.ScalarQueryParameter("entity", "STRING", entity),
            ]
        ),
    )

    rows = list(job.result())
    if not rows:
        raise RuntimeError("Active ingestion mapping not found")

    return dict(rows[0])


def load_yaml_contract(gcs_uri: str):
    if not gcs_uri.startswith("gs://"):
        raise ValueError("YAML contract must be gs://")

    _, path = gcs_uri.replace("gs://", "").split("/", 1)
    bucket_name, blob_path = path.split("/", 1)

    blob = storage.Client().bucket(bucket_name).blob(blob_path)
    return yaml.safe_load(blob.download_as_text())


def load_standards(bq, ops_project, ops_dataset, standard_id, layer):
    table = f"{ops_project}.{ops_dataset}.standards_definition"

    query = f"""
        SELECT column_name, expected_type, is_mandatory,
               is_partition_col, is_cluster_col
        FROM `{table}`
        WHERE standard_id=@standard_id AND layer=@layer
    """

    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("standard_id", "STRING", standard_id),
                bigquery.ScalarQueryParameter("layer", "STRING", layer),
            ]
        ),
    )

    return [
        {
            "column": r.column_name,
            "type": r.expected_type,
            "mandatory": r.is_mandatory,
            "partition": r.is_partition_col,
            "cluster": r.is_cluster_col,
        }
        for r in job.result()
    ]


def compute_next_batch_id(bq, project, dataset, table):
    q = f"SELECT COALESCE(MAX(meta_batch_id),'0') AS m FROM `{project}.{dataset}.{table}`"
    rows = list(bq.query(q).result())
    return str(int(rows[0]["m"]) + 1)


def load_runtime_metadata(
    *,
    env,
    domain,
    system,
    entity,
    runner_project,
    runner_sa,
    ops_project,
    ops_dataset,
):
    bq = get_bq_client(runner_project, runner_sa)

    mapping = load_ingestion_mapping(
        bq, ops_project, ops_dataset, env, domain, system, entity
    )

    yaml_contract = load_yaml_contract(mapping["gcs_yaml_ingestion_config"])

    standards = load_standards(
        bq, ops_project, ops_dataset, "BRONZE_V1", "BRONZE"
    )

    batch_id = compute_next_batch_id(
        bq,
        mapping["target_project_id"],
        mapping["target_dataset_id"],
        mapping["target_table_name"],
    )

    return {
        "env": env,
        "domain": domain,
        "system": system,
        "entity": entity,
        "target_project": mapping["target_project_id"],
        "target_dataset": mapping["target_dataset_id"],
        "target_table": mapping["target_table_name"],
        "contract": yaml_contract,          # YAML now
        "standards": standards,
        "meta_batch_id": batch_id,
        "job_timestamp": datetime.now(timezone.utc),
        "dlq_method": mapping["dlq_method"],
    }
