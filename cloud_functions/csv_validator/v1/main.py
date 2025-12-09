import re
from google.cloud import storage

# Example pattern: synthea_conditions_20250101.csv
FILENAME_PATTERN = re.compile(r"^([a-zA-Z0-9]+)_([a-zA-Z0-9]+)_(\d{8})\.csv$")

def validate_gcs_event(event, context):
    bucket_name = event["bucket"]
    file_name = event["name"]

    print(f"📥 Incoming file: gs://{bucket_name}/{file_name}")

    # Validate the filename
    match = FILENAME_PATTERN.match(file_name)
    if not match:
        print("❌ INVALID filename format.")
        print("Expected: <system>_<table>_<YYYYMMDD>.csv")
        return

    system, table, date = match.groups()
    print(f"✅ File accepted. System={system}, Table={table}, Date={date}")

    # OPTIONAL: Validate date is valid YYYYMMDD
    # OPTIONAL: Validate table exists in contracts bucket
    # OPTIONAL: Validate CSV header once added

    return "OK"



from main import validate_gcs_event

fake_event = {
    "bucket": "test-bucket",
    "name": "synthea_conditions_20250101.csv"
}

validate_gcs_event(fake_event, None)


