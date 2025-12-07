#!/usr/bin/env python3
import argparse
import sys
import re
from google.cloud import bigquery
from google.auth import impersonated_credentials
from google.auth.transport.requests import Request
import sqlglot
import google.auth



# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------
STANDARDS_PROJECT = "prj-lbd-shared-np"
STANDARDS_DATASET = "ops_metadata"
STANDARDS_TABLE = "standards_definition"

TARGET_STANDARD_ID = "BRONZE_V1"
TARGET_LAYER = "BRONZE"

# ---------------------------------------------
# AUTH + IMPERSONATION
# ---------------------------------------------

def get_impersonated_bq_client(target_sa, target_project):
    print(f"DEBUG: Impersonating SA: {target_sa}")

    source_credentials, _ = google.auth.default()

    impersonated = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_sa,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=3600,
    )

    return bigquery.Client(project=target_project, credentials=impersonated)


# ---------------------------------------------
# TYPE NORMALIZATION
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


# ---------------------------------------------
# SQL PREPROCESS
# ---------------------------------------------

def preprocess_sql(ddl_text):
    ddl_text = re.sub(r"\{\{.*?\}\}", "TEMPLATE_TOKEN", ddl_text)
    ddl_text = re.sub(r"\$\{.*?\}", "TEMPLATE_TOKEN", ddl_text)
    ddl_text = re.sub(r"`([^`]*)`", lambda m: m.group(1).replace(".", "_"), ddl_text)
    return ddl_text


# ---------------------------------------------
# MANUAL PARTITION / CLUSTER EXTRACTION (NEVER REMOVE AGAIN)
# ---------------------------------------------

def extract_partition_and_cluster(ddl_text):
    # ---- PARTITION BY ---------------------------------
    part_match = re.search(r"PARTITION\s+BY\s+([^\n;]+)", ddl_text, re.IGNORECASE)
    partitions = set()

    if part_match:
        expr = part_match.group(1).strip()
        inner = re.findall(r"\(?([A-Za-z_][A-Za-z0-9_]*)\)?", expr)
        if inner:
            partitions.add(inner[-1].lower())

    # ---- CLUSTER BY ----------------------------------
    cluster_match = re.search(r"CLUSTER\s+BY\s+([^\n;]+)", ddl_text, re.IGNORECASE)
    clusters = set()

    if cluster_match:
        cols = cluster_match.group(1)
        for col in cols.split(","):
            clusters.add(col.strip().lower())

    return partitions, clusters


# ---------------------------------------------
# LOAD STANDARDS TABLE
# ---------------------------------------------

def load_standards(bq_client):
    table_ref = f"{STANDARDS_PROJECT}.{STANDARDS_DATASET}.{STANDARDS_TABLE}"

    query = f"""
        SELECT column_name, expected_type, is_mandatory, is_partition_col, is_cluster_col
        FROM `{table_ref}`
        WHERE standard_id = '{TARGET_STANDARD_ID}'
          AND layer = '{TARGET_LAYER}'
    """

    rows = list(bq_client.query(query).result())
    print(f"DEBUG: Loaded {len(rows)} standards rows")

    required_columns = {}
    required_partitions = set()
    required_clusters = set()

    for r in rows:
        col = r.column_name.lower()
        required_columns[col] = r.expected_type.upper()

        if r.is_partition_col:
            required_partitions.add(col)

        if r.is_cluster_col:
            required_clusters.add(col)

    return required_columns, required_partitions, required_clusters


# ---------------------------------------------
# PARSE SQL DDL STRUCTURE
# ---------------------------------------------

def parse_sql_file(sql_file):
    with open(sql_file, "r") as f:
        ddl_text = f.read()

    raw_text = preprocess_sql(ddl_text)

    try:
        parsed = sqlglot.parse_one(raw_text, read="bigquery")
    except Exception as e:
        print(f"::error::Unable to parse SQL file {sql_file}: {e}")
        sys.exit(1)

    ddl_columns = {}
    for coldef in parsed.find_all(sqlglot.expressions.ColumnDef):
        col_name = coldef.name.lower()
        kind = coldef.args.get("kind")
        ddl_columns[col_name] = normalize_type(kind.sql()) if kind else None

    manual_part, manual_cluster = extract_partition_and_cluster(raw_text)
    return ddl_columns, manual_part, manual_cluster


# ---------------------------------------------
# VALIDATE SQL AGAINST STANDARDS
# ---------------------------------------------

def validate_sql_ddl(sql_file, required_cols, required_parts, required_clusters):
    ddl_cols, ddl_partitions, ddl_clusters = parse_sql_file(sql_file)

    # Missing mandatory columns
    missing = [c for c in required_cols if c not in ddl_cols]
    if missing:
        print(f"Error: DDL missing mandatory columns: {missing}")
        sys.exit(1)

    # Datatype mismatches
    mismatches = []
    for col, exp_type in required_cols.items():
        if ddl_cols[col] != exp_type:
            mismatches.append(f"{col}: expected {exp_type}, found {ddl_cols[col]}")

    if mismatches:
        print("Error: DDL has datatype mismatches:")
        for m in mismatches:
            print(" - " + m)
        sys.exit(1)

    # Partition check
    if required_parts and not ddl_partitions:
        print(f"Error: DDL missing PARTITION BY. Required: {required_parts}")
        sys.exit(1)

    if required_parts and not (ddl_partitions & required_parts):
        print(f"Error: DDL wrong partition column. Required: {required_parts}, Found: {ddl_partitions}")
        sys.exit(1)

    # Cluster check
    if required_clusters and not ddl_clusters:
        print(f"Error: DDL missing CLUSTER BY. Required: {required_clusters}")
        sys.exit(1)

    if required_clusters and not required_clusters.issubset(ddl_clusters):
        missing = required_clusters - ddl_clusters
        print(f"Error: DDL missing required cluster columns: {missing}")
        sys.exit(1)

    print("INFO: SQL DDL validation passed.")


# ---------------------------------------------
# MAIN
# ---------------------------------------------

def main(sql_file, project, dataset):
    target_sa = f"project-service-account@{project}.iam.gserviceaccount.com"

    # Always use impersonation when in CI
    bq_client = get_impersonated_bq_client(target_sa, project)

    # Identity check
    try:
        ident_query = "SELECT SESSION_USER()"
        result = list(bq_client.query(ident_query).result())
        print("DEBUG: BigQuery sees identity:", result[0][0])
    except Exception as e:
        print("DEBUG ERROR: Unable to detect identity:", e)

    required_cols, required_parts, required_clusters = load_standards(bq_client)

    validate_sql_ddl(sql_file, required_cols, required_parts, required_clusters)

    print("INFO: All validation checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql_file", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    args = parser.parse_args()

    main(args.sql_file, args.project, args.dataset)
