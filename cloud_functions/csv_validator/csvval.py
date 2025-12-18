
import pandas as pd 
from pandas.errors import ParserError

ow_limit = 10
delimiter = '|'

try:
    df = pd.read_csv(f, sep=delimiter, nrows=row_limit)

    # Check delimiter 
    if df.shape[1] <= 1:
        print(f"Suspicious Geometry: Found {df.shape[1]} columns. Possible delimiter mismatch.")

    # Check Header for missing columns
    header = df.columns.tolist()
    unnamed_cols = [c for c in header if str(c).startswith("Unnamed:")]

    if len(unnamed_cols) > 0:
        print(f"Garbage Header: Found {len(unnamed_cols)} 'Unnamed' columns.")
    else:
        ctx.extracted_headers = header

    # Check for "Replacement Characters" (The  symbol)
    if df.astype(str).apply(lambda x: x.str.contains('\ufffd')).any().any():
        print("Encoding Error: Found Unicode replacement characters ().")

    # CHECK BINARY/NULL BYTES
    if df.astype(str).apply(lambda x: x.str.contains('\x00')).any().any():
        print("Binary Data Detected: Found null bytes in text.")

except ParserError as e:
        # This catches "Expected 1 fields in line 3, saw 3"
    print (f"STRUCTURAL FAILURE: CSV Parsing Error - {str(e)}")
        
except UnicodeDecodeError as e:
    # This catches binary files or bad encoding
    print (f"ENCODING FAILURE: File is not valid text - {str(e)}")
    
except Exception as e:
    # Catch-all for other issues (permissions, etc.)
    print (f"READ FAILURE: {str(e)}")