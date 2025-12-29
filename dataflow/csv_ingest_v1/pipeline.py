# dataflow/pipeline.py

import csv
from io import StringIO
import apache_beam as beam

from common.validation import (
    ValidateAndParseDoFn,
    TAG_ERR_SCHEMA,
    TAG_ERR_TYPE,
    TAG_ERR_PK,
)
from common.dedup import compute_pk_hash, dedup_within_batch
from common.dlq import build_dlq_record

from common.read_csv import read_csv  # your module has read_csv(p, path)
from io.write_bq import write_good_rows, write_bq_dlq
from io.write_gcs import write_gcs_dlq
from io.write_pubsub import write_pubsub_dlq


def _ordered_columns(runtime_metadata: dict) -> list[str]:
    """
    Column order is driven by runtime metadata.
    Primary source: runtime_metadata["schema_columns"] (used by ValidateAndParseDoFn).
    """
    schema_cols = runtime_metadata.get("schema_columns")
    if isinstance(schema_cols, dict) and schema_cols:
        return list(schema_cols.keys())

    # Fallbacks (keep it defensive; your loader may populate differently)
    yaml_contract = runtime_metadata.get("yaml_contract", {}) or {}
    schema = yaml_contract.get("schema", {}) or {}

    cols = schema.get("columns")
    if isinstance(cols, dict) and cols:
        return list(cols.keys())
    if isinstance(cols, list) and cols and isinstance(cols[0], str):
        return cols

    raise ValueError(
        "Could not determine column order from runtime_metadata "
        "(expected schema_columns dict or yaml_contract.schema.columns)."
    )


def _parse_csv_line(line: str, columns: list[str]) -> dict:
    """
    Turn a single CSV line into a dict using metadata-driven column order.
    This keeps parsing in the pipeline layer; validation/type enforcement stays in ValidateAndParseDoFn.
    """
    # Handle empty lines safely
    if not line or not line.strip():
        return {}

    row = next(csv.reader(StringIO(line)))
    # If the row is shorter/longer, validation will catch schema drift later;
    # but we still build best-effort dict here.
    out = {}
    for i, col in enumerate(columns):
        out[col] = row[i] if i < len(row) else None
    # Keep any extra columns (schema drift) if present
    if len(row) > len(columns):
        for j in range(len(columns), len(row)):
            out[f"_extra_{j-len(columns)+1}"] = row[j]
    return out


def _attach_meta_columns(row: dict, runtime_metadata: dict) -> dict:
    """
    Attach job-level meta_* columns from runtime_metadata onto each row.
    Your loader already computes meta_batch_id, meta_ingest_timestamp, etc.
    """
    if not row:
        return row

    for k, v in runtime_metadata.items():
        if k.startswith("meta_") and k not in row:
            row[k] = v
    return row


def _normalize_dlq(pcoll, runtime_metadata: dict, category: str):
    return pcoll | f"BuildDLQRecord_{category}" >> beam.Map(
        lambda err: build_dlq_record(
            err,
            runtime_metadata=runtime_metadata,
            error_category=category,
        )
    )


def build_pipeline(p: beam.Pipeline, runtime_metadata: dict):
    """
    Build the full pipeline graph using ONLY runtime_metadata as control plane.
    No argparse here. The only external input to the job is file_name upstream,
    which metadata_loader turns into runtime_metadata.
    """

    input_path = runtime_metadata["input_file"]  # your code uses this key downstream
    pk_columns = runtime_metadata.get("pk_columns") or []
    dlq_methods = {
        m.strip().lower()
        for m in (runtime_metadata.get("dlq_method") or "").split(",")
        if m.strip()
    }

    columns = _ordered_columns(runtime_metadata)

    # 1) Read raw CSV lines (your read_csv module)
    lines = read_csv(p, input_path)

    # Optional header handling driven by metadata
    if runtime_metadata.get("csv_has_header", True):
        header_sig = set([c.lower() for c in columns])

        def _is_header(line: str) -> bool:
            try:
                cells = next(csv.reader(StringIO(line)))
                return set([c.strip().lower() for c in cells]) == header_sig
            except Exception:
                return False

        lines = lines | "DropHeaderRow" >> beam.Filter(lambda line: not _is_header(line))

    # 2) Parse to dict + attach meta columns
    raw_rows = (
        lines
        | "ParseCsvToDict" >> beam.Map(_parse_csv_line, columns=columns)
        | "AttachMetaColumns" >> beam.Map(_attach_meta_columns, runtime_metadata=runtime_metadata)
        | "DropEmptyRows" >> beam.Filter(lambda r: bool(r))
    )

    # 3) Validate + parse types (your DoFn + your tags)
    validated = (
        raw_rows
        | "ValidateAndParse" >> beam.ParDo(
            ValidateAndParseDoFn(runtime_metadata)
        ).with_outputs(
            TAG_ERR_SCHEMA,
            TAG_ERR_TYPE,
            TAG_ERR_PK,
            main="good",
        )
    )

    good = validated.good
    schema_err = validated[TAG_ERR_SCHEMA]
    type_err = validated[TAG_ERR_TYPE]
    pk_err = validated[TAG_ERR_PK]

    # 4) Compute PK hash + dedup within batch (your functions)
    good_hashed = good | "ComputePKHash" >> beam.Map(compute_pk_hash, pk_columns=pk_columns)
    good_deduped = good_hashed | "DedupWithinBatch" >> dedup_within_batch()

    # 5) Write good rows (BQ bronze)
    write_good_rows(good_deduped, runtime_metadata)

    # 6) DLQ routing (metadata-driven)
    # Normalize validation errors into DLQ_V1 schema
    schema_dlq = _normalize_dlq(schema_err, runtime_metadata, "SCHEMA")
    type_dlq = _normalize_dlq(type_err, runtime_metadata, "TYPE")
    pk_dlq = _normalize_dlq(pk_err, runtime_metadata, "PK")

    # NOTE: Your dlq.py expects a BQ error stream too (tags["BQ"]).
    # In the code you uploaded, the sinks (GCS/PubSub) are fed from *BQ DLQ*.
    # If/when you add a real BQ-write error stream, plug it in here.
    bq_dlq = (
        p
        | "EmptyBQDLQStream" >> beam.Create([])
    )

    # BQ DLQ (queryable): write schema/type/pk errors
    if "bq" in dlq_methods:
        write_bq_dlq(schema_dlq, runtime_metadata)
        write_bq_dlq(type_dlq, runtime_metadata)
        write_bq_dlq(pk_dlq, runtime_metadata)

    # GCS/PubSub DLQ (replay/alert): feed BQ-write failures (or empty until wired)
    if "gcs" in dlq_methods:
        write_gcs_dlq(bq_dlq, runtime_metadata)

    if "pubsub" in dlq_methods:
        write_pubsub_dlq(bq_dlq, runtime_metadata)

    return {
        "good": good_deduped,
        "dlq_schema": schema_dlq,
        "dlq_type": type_dlq,
        "dlq_pk": pk_dlq,
        "dlq_bq": bq_dlq,
    }
