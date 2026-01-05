import apache_beam as beam
import hashlib
import json


def compute_pk_hash(row: dict, pk_columns: list | None):
    """
    Compute deterministic PK hash.

    Rules:
    - If pk_columns provided → hash only those columns (ordered)
    - If pk_columns missing/empty → hash entire row
    """

    if pk_columns:
        values = [str(row.get(col, "")) for col in pk_columns]
    else:
        # Full-row hash fallback (stable ordering)
        values = json.dumps(row, sort_keys=True)

    raw = "|".join(values) if isinstance(values, list) else values

    row["meta_pk_hash"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return row


def dedup_within_batch(pcoll):
    """
    Deduplicate records within the current window (hourly or global).

    Assumes:
    - Windowing is already applied upstream
    - `meta_pk_hash` exists on each row
    - Keeps the FIRST record per PK per window
    """

    return (
        pcoll
        | "KeyByPKHash" >> beam.Map(
            lambda row: (row["meta_pk_hash"], row)
        )
        | "GroupByPKHash" >> beam.GroupByKey()
        | "KeepFirstPerPK" >> beam.Map(
            lambda kv: kv[1][0]
        )
    )
