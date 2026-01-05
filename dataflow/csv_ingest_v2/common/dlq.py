# dataflow/common/dlq.py

import json
import datetime


def build_dlq_record(
    error_record: dict,
    runtime_metadata: dict,
    error_category: str,
):
    """
    Normalize an error into DLQ_V1 schema.
    Sink-agnostic.
    """

    return {
        # Identity
        "meta_batch_id": runtime_metadata["batch_id"],
        "meta_pk_hash": error_record.get("meta_pk_hash"),

        # Classification
        "error_stage": "DATAFLOW",
        "error_category": error_category,
        "error_code": error_record.get("error_code", "UNKNOWN"),
        "error_message": error_record.get("error_message", ""),

        # Payload
        "payload_type": "ROW",
        "payload": json.dumps(error_record.get("payload", error_record)),
        "schema_version": runtime_metadata["schema_version"],

        # Audit
        "meta_ingest_timestamp": datetime.datetime.utcnow().isoformat(),
        "meta_source_system": runtime_metadata["system"],
        "meta_source_filename": runtime_metadata["input_file"],
    }


def route_dlq(
    results,
    runtime_metadata,
    tags,
    write_bq_dlq,
    write_gcs_dlq,
    write_pubsub_dlq,
):
    """
    Fanout DLQ streams based on dlq_method.
    """

    dlq_methods = set(
        m.strip().lower()
        for m in runtime_metadata["dlq_method"].split(",")
    )

    def normalize(tag):
        return lambda pcoll: pcoll | f"Normalize{tag}" >> (
            lambda x: build_dlq_record(
                x, runtime_metadata=runtime_metadata, error_category=tag
            )
        )

    schema_dlq = results[tags["SCHEMA"]] | "SchemaDLQ" >> normalize("SCHEMA")
    type_dlq   = results[tags["TYPE"]]   | "TypeDLQ"   >> normalize("TYPE")
    pk_dlq     = results[tags["PK"]]     | "PKDLQ"     >> normalize("PK")
    bq_dlq     = results[tags["BQ"]]     | "BQDLQ"     >> normalize("BQ")

    if "bq" in dlq_methods:
        write_bq_dlq(schema_dlq, runtime_metadata)
        write_bq_dlq(type_dlq, runtime_metadata)
        write_bq_dlq(pk_dlq, runtime_metadata)

    if "gcs" in dlq_methods:
        write_gcs_dlq(bq_dlq, runtime_metadata)

    if "pubsub" in dlq_methods:
        write_pubsub_dlq(bq_dlq, runtime_metadata)
