import logging
from google.cloud import storage
import pandas as pd
from pandas.errors import ParserError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s:%(funcName)s → %(message)s"
)

logger = logging.getLogger(__name__)

storage_client = storage.Client()


def csv_content_validation(ctx):
    """
    If file_size is under 1mb check contents 
    """
    bucket = storage_client.bucket(ctx.bucket)
    blob = bucket.blob(ctx.file_path)

    try: 
        with blob.open("r") as f:
            
            df = pd.read_csv(f, sep=ctx.delimiter, nrows=ctx.row_limit)

            # Check delimiter 
            if df.shape[1] <= 1:
                ctx.add_error(f"Suspicious Geometry: Found {df.shape[1]} columns. Possible delimiter mismatch.")

            # Check Header for missing columns
            header = df.columns.tolist()
            unnamed_cols = [c for c in header if str(c).startswith("Unnamed:")]

            if len(unnamed_cols) > 0:
                ctx.add_error(f"Garbage Header: Found {len(unnamed_cols)} 'Unnamed' columns.")
            else:
                ctx.extracted_headers = header

            # Check for "Replacement Characters" (The  symbol)
            if df.astype(str).apply(lambda x: x.str.contains('\ufffd')).any().any():
                ctx.add_error("Encoding Error: Found Unicode replacement characters ().")

            # CHECK BINARY/NULL BYTES
            if df.astype(str).apply(lambda x: x.str.contains('\x00')).any().any():
                ctx.add_error("Binary Data Detected: Found null bytes in text.")

    except ParserError as e:
            # This catches "Expected 1 fields in line 3, saw 3"
        ctx.add_error (f"STRUCTURAL FAILURE: CSV Parsing Error - {str(e)}")
            
    except UnicodeDecodeError as e:
        # This catches binary files or bad encoding
        ctx.add_error (f"ENCODING FAILURE: File is not valid text - {str(e)}")
        
    except Exception as e:
        # Catch-all for other issues (permissions, etc.)
        ctx.add_error (f"READ FAILURE: {str(e)}")


def csv_schema_drift_checks(ctx):

    import json, re

    contract_uri = ctx.contract_gcs_path

    match = re.match(r'gs://([^/]+)/(.+)', )
    if not match:
        msg = f"Schema contract not found: {contract_uri}"
        ctx.add_error(msg)
        return False
    
    config_bucket = match.group(1)
    contract_blob = match.group(2)

    # 2. Initialize Client
    storage_client = storage.Client()
    bucket = storage_client.bucket(config_bucket)
    blob = bucket.blob(contract_blob)

    # 3. Download and Parse
    try:
        data_str = blob.download_as_text() 
        contract_json = json.loads(data_str) 
    except Exception as e:
        msg = f"Failed to read schema contract: gs://{config_bucket}/{contract_blob}"
        ctx.add_error(msg)
        return False

    # Extract schema fields
    contract_fields = {f["name"].lower(): f for f in contract_json.get("fields", [])}
    contract_cols = set(contract_fields.keys())

    # SCHEMA DRIFT CALCULATIONS

    missing_cols = list(contract_cols - ctx.extracted_headers)
    extra_cols = list(ctx.extracted_headers - contract_cols)

    if missing_cols:
        ctx.add_error(f"Missing Columns: {missing_cols}")
        return False

    if extra_cols:
        ctx.add_error(f"Extra Columns: {extra_cols}")
        return False

    logger.info(f"No schema drift detected for {ctx.file_path}")
    return True

    # type_mismatches = []
    # # OPTIONAL: type checks only if contract has "type"
    # for col in contract_cols & ctx.extracted_headers:
    #     expected = contract_fields[col].get("type")
    # if type_mismatches:
    #     msg = f"Type mismatches: {type_mismatches}"
    #     ctx.add_error(msg)
