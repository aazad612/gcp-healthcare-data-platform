# dataflow/io/write_pubsub.py

import json
import apache_beam as beam
from apache_beam.io.gcp.pubsub import WriteToPubSub


def write_pubsub_dlq(pcoll, runtime_metadata):
    """
    Publish DLQ records to Pub/Sub for immediate attention.

    Assumes:
    - runtime_metadata["dlq_topic"] exists
    - Topic format: projects/<project>/topics/<topic>
    """

    topic = runtime_metadata["dlq_topic"]

    if not topic:
        raise ValueError("dlq_topic not provided in runtime_metadata")

    (
        pcoll
        | "DLQToJsonPubSub" >> beam.Map(json.dumps)
        | "WriteDLQToPubSub" >> WriteToPubSub(
            topic=topic,
            with_attributes=False
        )
    )
