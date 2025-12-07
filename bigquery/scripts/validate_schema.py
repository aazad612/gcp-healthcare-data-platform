#!/usr/bin/env python3
import argparse
import sys
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sqlglot
import re
import os

# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------
STANDARDS_PROJECT = "prj-lbd-shared-np"
STANDARDS_DATASET = "ops_metadata"
STANDARDS_TABLE = "standards_definition"

TARGET_STANDARD_ID = "BRONZE_V1"
TARGET_LAYER = "BRONZE"


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
# SQL PREPROCESSOR
# ---------------------------------------------

def preprocess_sql(ddl_text):
    ddl_text = re.sub(r"\{\{.*?\}\}", "TOKEN", ddl_text)
    ddl_text = re.sub(r"\$\{.*?\}\}", "TOKEN", ddl_text)
    ddl_text = re.sub(r"`([^`]*)`", lambda m: m.group(1).replace(".", "_"), ddl_text)
    return ddl_text


# ---------------------------------------------
# PARTITION / CLUSTER REGEX EXTRACTION
# ---------------------------------------------

def extract_partition_and_cluster(ddl_raw):
    partition_cols = set()
    cluster_cols = set()

    # ---- PARTITION BY ----
    part_match = re.search(
        r"PARTITION\s+BY\s+(.+?)(?:\n|;)",
        ddl_raw,
        flags=re.IGNORECASE
    )

    if part_match:
        expr = part_match.group(1)
        tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)
        if tokens:
            partition_cols.add(tokens[-1].lower())

    # ---- CLUSTER BY ---- (MULTI-LINE SAFE)
    cluster_match = re.search(
        r"CLUSTER\s+BY\s+(.+?)(?:OPTIONS|\nPARTITION|\nCLUSTER|;|$)",
        ddl_raw,
        flags=re.IGNORECASE | re.DOTALL
    )

    if cluster_match:
        expr = cluster_match.group(1)
        cols = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)
        for c in cols:
            cluster_cols.add(c.lower())

    return partition_cols, cluster_cols


# ---------------------------------------------
# LOAD BRONZE STANDARDS
# ---------------------------------------------

def load_standards():
    client = bigquery.Client(project=STANDARDS_PROJECT)
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

    rows = list(client.query(query).result())
    if not rows:
        print(f"::error::No standards found for {TARGET_STANDARD_ID}")
        sys.exit(1)

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
# DERIVE TABLE NAME FROM FILE NAME
# ---------------------------------------------

def derive_table_name(sql_file_path):
    base = os.path.basename(sql_file_path)
    name = os.path.splitext(base)[0]
    return name.lower()


# ---------------------------------------------
# PARSE SQL DDL FOR COLUMN DEFINITIONS
# ---------------------------------------------

def parse_sql_columns(sql_file):
    with open(sql_file, "r") as f:
        ddl_raw = f.read()

    ddl_clean = preprocess_sql(ddl_raw)

    parsed = sqlglot.parse_one(ddl_clean, read="bigquery", error_level="ignore")

    ddl_columns = {}
    for coldef in parsed.find_all(sqlglot.expressions.ColumnDef):
        col = coldef.name.lower()
        type_expr = coldef.args.get("kind")
        if type_expr:
            ddl_columns[col] = normalize_type(type_expr.sql())

    ddl_partitions, ddl_clusters = extract_partition_and_cluster(ddl_raw)

    return ddl_columns, ddl_partitions, ddl_clusters


# ---------------------------------------------
# VALIDATE SQL DDL
# ---------------------------------------------

def validate_sql_ddl(sql_file, required_columns, required_partitions, required_clusters):

    ddl_columns, ddl_partitions, ddl_clusters = parse_sql_columns(sql_file)

    # Missing columns
    missing_cols = [c for c in required_columns if c not in ddl_columns]
    if missing_cols:
        print(f"::error::DDL missing mandatory columns: {missing_cols}")
        sys.exit(1)

    # Type mismatches
    mismatches = []
    for col, exp in required_columns.items():
        if ddl_columns[col] != exp:
            mismatches.append(f"{col}: expected {exp}, found {ddl_columns[col]}")

    if mismatches:
        print("::error::DDL datatype mismatches:\n" + "\n".join(mismatches))
        sys.exit(1)

    # Partition check
    if required_partitions and not ddl_partitions:
        print(f"::error::DDL missing PARTITION BY. Required: {required_partitions}")
        sys.exit(1)

    if required_partitions and not (required_partitions & ddl_partitions):
        print(f"::error::DDL incorrect partition column. Found {ddl_partitions}, expected {required_partitions}")
        sys.exit(1)

    # Cluster check
    if required_clusters and not ddl_clusters:
        print(f"::error::DDL missing CLUSTER BY. Required: {required_clusters}")
        sys.exit(1)

    if required_clusters and not required_clusters.issubset(ddl_clusters):
        print(f"::error::DDL missing required cluster columns: {required_clusters - ddl_clusters}")
        sys.exit(1)

    print("INFO: SQL DDL validation passed.")


# ---------------------------------------------
# VALIDATE EXISTING BQ TABLE
# ---------------------------------------------

def debug_identity(project_id):
    """
    Runs SELECT SESSION_USER() to confirm which identity BigQuery sees.
    """
    try:
        client = bigquery.Client(project=project_id)
        debug_query = "SELECT SESSION_USER() as identity"
        result = list(client.query(debug_query).result())
        print(f"DEBUG: BigQuery sees identity: {result[0].identity}")
    except Exception as e:
        print(f"DEBUG ERROR: Unable to detect identity: {e}")




def validate_existing_table(project, dataset, table, required_columns, required_partitions, required_clusters):

    client = bigquery.Client(project=project)
    ref = f"{project}.{dataset}.{table}"

    try:
        bq_table = client.get_table(ref)
    except NotFound:
        print("INFO: Table does not exist yet — DDL will create it.")
        return

    existing_cols = {f.name.lower(): normalize_type(f.field_type) for f in bq_table.schema}

    missing = [c for c in required_columns if c not in existing_cols]
    if missing:
        print(f"::error::Existing table missing mandatory columns: {missing}")
        sys.exit(1)

    mismatches = []
    for col, exp in required_columns.items():
        if existing_cols[col] != exp:
            mismatches.append(f"{col}: expected {exp}, found {existing_cols[col]}")

    if mismatches:
        print("::error::Existing table datatype mismatches:\n" + "\n".join(mismatches))
        sys.exit(1)

    # Partition check
    if required_partitions:
        if not bq_table.time_partitioning:
            print("::error::Existing table missing partitioning.")
            sys.exit(1)

        col = bq_table.time_partitioning.field.lower()
        if col not in required_partitions:
            print(f"::error::Incorrect partition column. Found {col}, required {required_partitions}")
            sys.exit(1)

    # Cluster check
    if required_clusters:
        existing_clusters = set(c.lower() for c in (bq_table.clustering_fields or []))
        missing_clusters = required_clusters - existing_clusters

        if missing_clusters:
            print(f"::error::Existing table missing required cluster columns: {missing_clusters}")
            sys.exit(1)

    print("INFO: Existing table validation passed.")


# ---------------------------------------------
# MAIN
# ---------------------------------------------

def main(sql_file, project, dataset):

    table = derive_table_name(sql_file)

    required_cols, required_parts, required_clust = load_standards()

    validate_sql_ddl(sql_file, required_cols, required_parts, required_clust)

    validate_existing_table(project, dataset, table,
                            required_cols, required_parts, required_clust)

    print("INFO: All validation checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql_file", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)

    args = parser.parse_args()

    debug_identity(args.project)

    main(args.sql_file, args.project, args.dataset)
