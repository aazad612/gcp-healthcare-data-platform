# dataflow/common/validation.py

import re
import uuid
import datetime
from apache_beam import DoFn, pvalue


TAG_ERR_SCHEMA = "schema_drift"
TAG_ERR_TYPE = "type_error"
TAG_ERR_PK = "pk_error"


def is_ascii(value: str) -> bool:
    try:
        value.encode("ascii")
        return True
    except Exception:
        return False


def parse_value(value, expected_type):
    if value is None:
        return None

    if expected_type == "STRING":
        return str(value)

    if expected_type == "INT":
        return int(value)

    if expected_type == "FLOAT":
        return float(value)

    if expected_type == "BOOLEAN":
        return str(value).lower() in ("true", "1", "t", "yes")

    if expected_type == "DATE":
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()

    if expected_type == "TIMESTAMP":
        return datetime.datetime.fromisoformat(value)

    return value


class ValidateAndParseDoFn(DoFn):
    def __init__(self, runtime_metadata: dict):
        self.schema = runtime_metadata["schema_columns"]
        self.quality = runtime_metadata.get("quality_checks", [])
        self.pk_columns = runtime_metadata.get("pk_columns", [])

    def process(self, row: dict):
        # ---- Schema drift (missing / extra columns) ----
        row_cols = set(row.keys())
        schema_cols = set(self.schema.keys())

        if row_cols != schema_cols:
            yield pvalue.TaggedOutput(
                TAG_ERR_SCHEMA,
                {
                    "error_code": "SCHEMA_DRIFT",
                    "error_message": f"Expected {schema_cols}, got {row_cols}",
                    "payload": row,
                },
            )
            return

        parsed = {}

        # ---- Type validation ----
        for col, col_def in self.schema.items():
            try:
                parsed[col] = parse_value(row.get(col), col_def["type"])
            except Exception as e:
                yield pvalue.TaggedOutput(
                    TAG_ERR_TYPE,
                    {
                        "error_code": "TYPE_ERROR",
                        "error_message": f"{col}: {str(e)}",
                        "payload": row,
                    },
                )
                return

        # ---- PK validation ----
        for pk in self.pk_columns:
            if not parsed.get(pk):
                yield pvalue.TaggedOutput(
                    TAG_ERR_PK,
                    {
                        "error_code": "PK_NULL",
                        "error_message": f"PK column {pk} is null",
                        "payload": row,
                    },
                )
                return

        # ---- Constraint checks (YAML-driven) ----
        for rule in self.quality:
            col = rule["column"]
            val = parsed.get(col)

            if rule["rule"] == "not_null" and val is None:
                yield pvalue.TaggedOutput(TAG_ERR_TYPE, {
                    "error_code": "NOT_NULL",
                    "error_message": rule["error_msg"],
                    "payload": row,
                })
                return

            if rule["rule"] == "regex_match" and val is not None:
                if not re.match(rule["pattern"], str(val)):
                    yield pvalue.TaggedOutput(TAG_ERR_TYPE, {
                        "error_code": "REGEX",
                        "error_message": rule["error_msg"],
                        "payload": row,
                    })
                    return

            if rule["rule"] == "uuid_format" and val is not None:
                try:
                    uuid.UUID(str(val))
                except Exception:
                    yield pvalue.TaggedOutput(TAG_ERR_TYPE, {
                        "error_code": "UUID",
                        "error_message": rule["error_msg"],
                        "payload": row,
                    })
                    return

            if rule["rule"] == "ascii_only" and val is not None:
                if not is_ascii(str(val)):
                    yield pvalue.TaggedOutput(TAG_ERR_TYPE, {
                        "error_code": "NON_ASCII",
                        "error_message": rule["error_msg"],
                        "payload": row,
                    })
                    return

            if rule["rule"] == "allowed_values" and val is not None:
                if val not in rule["values"]:
                    yield pvalue.TaggedOutput(TAG_ERR_TYPE, {
                        "error_code": "INVALID_VALUE",
                        "error_message": rule["error_msg"],
                        "payload": row,
                    })
                    return

        # ---- Success ----
        yield parsed
