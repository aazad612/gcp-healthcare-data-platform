"""
validation.py

Row-level validation using YAML contract.
"""

from datetime import datetime
import re
import uuid


def cast_value(value, target_type):
    if value in ("", None):
        return None

    try:
        if target_type == "STRING":
            return str(value)
        if target_type == "INT64":
            return int(value)
        if target_type == "FLOAT64":
            return float(value)
        if target_type == "DATE":
            return datetime.strptime(value, "%Y-%m-%d").date()
        if target_type == "TIMESTAMP":
            return datetime.fromisoformat(value)
        if target_type == "BOOLEAN":
            return str(value).lower() in ("true", "1", "yes")
    except Exception:
        raise ValueError(f"Invalid {target_type}: {value}")

    return value


def apply_quality_rules(col, value, rules):
    if value is None:
        return

    rule = rules.get("rule")

    if rule == "regex_match":
        if not re.match(rules["pattern"], str(value)):
            raise ValueError(f"{col}: regex mismatch")

    if rule == "uuid_format":
        uuid.UUID(str(value))

    if rule == "allowed_values":
        if value not in rules["values"]:
            raise ValueError(f"{col}: invalid enum")


def validate_row(row, contract, standards, meta):
    clean = {}

    # --- Business schema ---
    schema = contract["schema"]
    quality = {r["column"]: r for r in contract.get("quality", {}).get("row_level_checks", [])}

    for col in schema["columns"]:
        name = col["name"]
        dtype = col["type"]
        mode = col.get("mode", "NULLABLE")

        raw = row.get(name)

        if mode == "REQUIRED" and (raw is None or raw == ""):
            return None, f"{name} is REQUIRED"

        value = cast_value(raw, dtype)

        if name in quality:
            apply_quality_rules(name, value, quality[name])

        clean[name] = value

    # --- Governance metadata ---
    enriched = dict(clean)

    for s in standards:
        c = s["column"]

        if c == "meta_row_uuid":
            enriched[c] = str(uuid.uuid4())
        elif c == "meta_batch_id":
            enriched[c] = meta["meta_batch_id"]
        elif c == "meta_ingest_timestamp":
            enriched[c] = meta["job_timestamp"]
        elif c == "meta_source_system":
            enriched[c] = meta["system"]
        elif c == "meta_dq_flag":
            enriched[c] = "VALID"
        elif c == "meta_created_by":
            enriched[c] = "dataflow"
        elif c == "meta_created_date":
            enriched[c] = meta["job_timestamp"]
        elif c == "meta_updated_by":
            enriched[c] = "dataflow"
        elif c == "meta_updated_date":
            enriched[c] = meta["job_timestamp"]

    return enriched, None
