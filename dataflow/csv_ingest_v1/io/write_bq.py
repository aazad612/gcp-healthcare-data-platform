# dataflow/io/write_bq.py

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


def write_good_rows(pcoll, runtime_metadata):
    """
    Write valid rows to target Bronze table.
    """

    table = (
        f"{runtime_metadata['target_project']}."
        f"{runtime_metadata['target_dataset']}."
        f"{runtime_metadata['target_table']}"
    )

    (
        pcoll
        | "WriteGoodRowsBQ" >> WriteToBigQuery(
            table=table,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )
    )


def write_bq_dlq(pcoll, runtime_metadata):
    """
    Write DLQ rows to <table_name>_bad in same dataset.
    """

    dlq_table = (
        f"{runtime_metadata['target_project']}."
        f"{runtime_metadata['target_dataset']}."
        f"{runtime_metadata['target_table']}_bad"
    )

    (
        pcoll
        | "WriteDLQRowsBQ" >> WriteToBigQuery(
            table=dlq_table,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )
    )
