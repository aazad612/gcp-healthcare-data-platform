# dataflow/io/write_gcs.py

import json
from google.cloud import storage
import apache_beam as beam
from apache_beam.io import WriteToText


def write_gcs_dlq(pcoll, runtime_metadata):
    """
    Write DLQ records to GCS as JSON lines.
    Used for non-queryable / replay-required failures.
    """

    base_path = (
        f"gs://{runtime_metadata['dlq_gcs_bucket']}/"
        f"{runtime_metadata['dlq_gcs_prefix']}/"
        f"{runtime_metadata['target_table']}"
    )

    (
        pcoll
        | "DLQToJson" >> beam.Map(json.dumps)
        | "WriteDLQToGCS" >> WriteToText(
            base_path,
            file_name_suffix=".json",
            shard_name_template="-SSSSS-of-NNNNN",
        )
    )


def move_input_file(runtime_metadata, status: str):
    """
    Move input file from incoming -> archived OR error.

    status: 'archived' | 'error'
    """

    input_uri = runtime_metadata["input_file"]

    if "/incoming/" not in input_uri:
        raise ValueError(f"Input file path does not contain /incoming/: {input_uri}")

    target_uri = input_uri.replace("/incoming/", f"/{status}/")

    # Parse GCS URIs
    def parse_gcs(uri):
        uri = uri.replace("gs://", "")
        bucket, blob = uri.split("/", 1)
        return bucket, blob

    src_bucket_name, src_blob_name = parse_gcs(input_uri)
    tgt_bucket_name, tgt_blob_name = parse_gcs(target_uri)

    client = storage.Client()

    src_bucket = client.bucket(src_bucket_name)
    src_blob = src_bucket.blob(src_blob_name)

    tgt_bucket = client.bucket(tgt_bucket_name)

    # Copy then delete (atomic-enough for GCS)
    src_bucket.copy_blob(
        src_blob,
        tgt_bucket,
        tgt_blob_name,
    )

    src_blob.delete()
