import re
import json
from google.cloud import storage
from google.cloud import pubsub_v1

# Pub/Sub topic for downstream processing
PUBSUB_TOPIC = "projects/PRJ_ID/topics/df-ingest-np-csv"

# Filename pattern: system-table-date.csv
FILENAME_REGEX = r"(?P<system>[a-zA-Z0-9_]+)-(?P<table>[a-zA-Z0-9_]+)-(?P<date>\d{8})\.csv"


def parse_filename(filename):
    match = re.match(FILENAME_REGEX, filename)
    if not match:
        return None
    return match.group("system"), match.group("table"), match.group("date")


def get_csv_header(bucket_name, file_path):
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(file_path)

    # Download only the first line — efficient even for 10GB files
    first_line = blob.download_as_text().splitlines()[0]
    return [col.strip() for col in first_line.split(",")]


def publish_to_pubsub(payload):
    publisher = pubsub_v1.PublisherClient()
    publisher.publish(PUBSUB_TOPIC, json.dumps(payload).encode("utf-8"))


def gcs_csv_validator(event, context):
    bucket = event["bucket"]
    file_path = event["name"]

    filename = file_path.split("/")[-1]

    # Validate filename
    parsed = parse_filename(filename)
    if not parsed:
        payload = {
            "file_path": file_path,
            "bucket": bucket,
            "error": "invalid_filename",
            "pattern_expected": FILENAME_REGEX,
        }
        publish_to_pubsub(payload)
        print("Invalid filename:", filename)
        return

    system, table, extract_date = parsed

    # Extract CSV header
    try:
        header = get_csv_header(bucket, file_path)
    except Exception as e:
        payload = {
            "file_path": file_path,
            "bucket": bucket,
            "system": system,
            "table": table,
            "extract_date": extract_date,
            "error": f"csv_header_read_failed: {str(e)}",
        }
        publish_to_pubsub(payload)
        print("CSV header error:", e)
        return

    # Build message for Dataflow
    payload = {
        "file_path": file_path,
        "bucket": bucket,
        "system": system,
        "table": table,
        "extract_date": extract_date,
        "header": header,
        "is_valid_filename": True,
    }

    publish_to_pubsub(payload)
    print("Published CSV metadata:", payload)
