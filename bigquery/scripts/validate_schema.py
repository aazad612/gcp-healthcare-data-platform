#!/usr/bin/env python3
import argparse
import sys
import re

from google.cloud import bigquery
import google.auth
from google.auth import impersonated_credentials

# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------

STANDARDS_PROJECT = "prj-lbd-shared-np"
STANDARDS_DATASET = "ops_metadata"
STANDARDS_TABLE = "standards_definition"

TARGET_STANDARD_ID = "BRONZE_V1"
TARGET_LAYER = "BRONZE"

# ---------------------------------------------
# IMPERSONATION CLIENT BUILDER
# ---------------------------------------------

def get_impersonated_bq_client(target_sa, project_id):
    """
    Always impersonate the deployment service account.
    This overrides whatever GHA WIF identity is active.
    """
    print(f"DEBUG: Impersonating SA: {target_sa}")

    source_credentials, _ = google.auth.default()

    target_creds = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_sa,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=3600,
    )

    return bigquery.Client(project=project_id, credentials=target_creds)


def debug_identity(client):
    """
    Runs SELECT SESSION_USER() to verify which identity BigQuery sees.
    """
    try:
        q = "SELECT SESSION_USER() AS identity"
        result = list(client.query(q).result())
        print(f"DEBUG: BigQuery sees identity: {result[0].identity}")
    except Exception as e:
        print(f"DEBUG ERROR: Unable to detect identity: {e}")


# ---------------------------------------------
# NORMALIZATION UTILS
# ---------------------------------------------

def normalize_type(t):
    if not t:
        return None
    t = t.upper()
    if t in ("TIMESTAMPTZ", "TIMESTAMPZ"):
        return "TIMESTAMP"
    if t == "TEXT":
        return "STRING"
    if t in ("INT", "INTEGER"):
        return "INT64"
    if t == "DATETIME":
        return "TIMESTAMP"
    return t

def preprocess_sql(ddl_text):
    ddl_text = re.sub(r"\{\{.*?\}\}", "TEMPLATE_TOKEN", ddl_text)
    ddl_text = re.sub(r"\$\{.*?\}", "TEMPLATE_TOKEN", ddl_text)
    ddl_text = re.sub(r"`([^`]*)`", lambda m: m.group(1).replace(".", "_"), ddl_text)
    return ddl_text


# ---------------------------------------------
# LOAD STANDARDS FROM SHARED PROJECT
# ---------------------------------------------

def load_standards(bq_client):
    table_ref = f"{STANDARDS_PROJECT}.{STANDARDS_DATASET}.{STANDARDS_TABLE}"

    query = f"""
        SELECT
            column_name,
            expected_type,
            is_mandatory,
            is_partition_col,
            is_cluster_col
        FROM `{table_ref}`
        WHERE
            standard_id = '{TARGET_STANDARD_ID}'
            AND layer = '{TARGET_LAYER}'
    """

    rows = list(bq_client.query(query).result())

    if not rows:
        print(f"::error::No standards found for {TARGET_STANDARD_ID} in {table_ref}")
        sys.exit(1)

    required_cols = {}
    required_parts = set()
    required_clust = set()

    for r in rows:
        col = r.column_name.lower()
        required_cols[col] = r.expected_type.upper()

        if r.is_partition_col:
            required_parts.add(col)

        if r.is_cluster_col:
            required_clust.add(col)

    print(f"DEBUG: Loaded {len(rows)} standards rows")
    return required_cols, required_parts, required_clust


# ---------------------------------------------
# PARSE AND VALIDATE SQL
# ---------------------------------------------

def parse_sql_file(sql_file):
    import sqlglot

    with open(sql_file, "r") as f:
        ddl_text = preprocess_sql(f.read())

    try:
        parsed = sqlglot.parse_one(ddl_text, read="bigquery")
    except Exception as e:
        print(f"::error::Unable to parse SQL file {sql_file}: {e}")
        sys.exit(1)

    ddl_columns = {}
    for coldef in parsed.find_all(sqlglot.expressions.ColumnDef):
        col_name = coldef.name.lower()
        type_expr = coldef.args.get("kind")
        if type_expr:
            ddl_columns[col_name] = normalize_type(type_expr.sql())

    # Extract PARTITION BY / CLUSTER BY
    ddl_partitions = set()
    ddl_clusters = set()

    part = parsed.find(sqlglot.expressions.Partition)
    if part and part.expressions:
        for expr in part.expressions:
            if hasattr(expr, "name"):
                ddl_partitions.add(expr.name.lower())
            elif expr.expressions:
                inner = expr.expressions[0]
                if hasattr(inner, "name"):
                    ddl_partitions.add(inner.name.lower())

    cluster = parsed.find(sqlglot.expressions.Cluster)
    if cluster and cluster.expressions:
        ddl_clusters = {expr.name.lower() for expr in cluster.expressions}

    return ddl_columns, ddl_partitions, ddl_clusters


def validate_sql_ddl(sql_file, required_cols, required_parts, required_clust):
    ddl_cols, ddl_parts, ddl_clust = parse_sql_file(sql_file)

    missing = [c for c in required_cols if c not in ddl_cols]
    if missing:
        print(f"::error::DDL missing mandatory columns: {missing}")
        sys.exit(1)

    mismatches = []
    for col, exp_type in required_cols.items():
        if ddl_cols[col] != exp_type:
            mismatches.append(f"{col}: expected {exp_type}, found {ddl_cols[col]}")

    if mismatches:
        print("::error::DDL datatype mismatches:")
        for m in mismatches:
            print("  - " + m)
        sys.exit(1)

    if required_parts and not ddl_parts:
        print(f"::error::DDL missing PARTITION BY. Required: {required_parts}")
        sys.exit(1)

    if required_parts and not (set(required_parts) & set(ddl_parts)):
        print(f"::error::Incorrect partition columns. Required: {required_parts}, Found: {ddl_parts}")
        sys.exit(1)

    if required_clust and not ddl_clust:
        print(f"::error::DDL missing CLUSTER BY. Required: {required_clust}")
        sys.exit(1)

    if required_clust and not required_clust.issubset(ddl_clust):
        missing = required_clust - ddl_clust
        print(f"::error::Missing cluster columns: {missing}")
        sys.exit(1)

    print("DEBUG: SQL DDL validation passed.")


# ---------------------------------------------
# MAIN
# ---------------------------------------------

def main(sql_file, project, dataset):
    target_sa = f"project-service-account@{project}.iam.gserviceaccount.com"

    # Build impersonated client for *shared project* standards lookup
    shared_client = get_impersonated_bq_client(target_sa, STANDARDS_PROJECT)
    print("DEBUG: Checking BigQuery identity (shared project):")
    debug_identity(shared_client)

    # Load standards
    required_cols, required_parts, required_clust = load_standards(shared_client)

    # Validate SQL file structure
    validate_sql_ddl(sql_file, required_cols, required_parts, required_clust)

    print("INFO: Validation successful.")
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql_file", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    args = parser.parse_args()

    main(args.sql_file, args.project, args.dataset)
