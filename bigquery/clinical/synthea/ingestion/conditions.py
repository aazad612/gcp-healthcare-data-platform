import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

# --- CONFIGURATION ---
# 1. Define your Column Names (Must match BigQuery Schema order)
HEADERS = ['patient_id', 'encounter_date', 'diagnosis_code'] 

# 2. Define the Target Table (project:dataset.table)
TABLE_SPEC = 'your-project:your_dataset.your_table'

# 3. Input File
INPUT_FILE = 'gs://your-bucket/path/to/file.csv'

def parse_csv_line(line):
    """
    Splits a CSV line by comma and maps it to the headers.
    Input: "101,2025-01-01,Flu"
    Output: {"patient_id": "101", "encounter_date": "2025-01-01", "diagnosis_code": "Flu"}
    """
    values = line.split(',')
    
    # Zip combines headers and values into a dictionary
    return dict(zip(HEADERS, values))

def run():
    # Setup Pipeline Options (The "Arguments")
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'your-project-id'
    google_cloud_options.job_name = 'barebones-ingestion-job'
    google_cloud_options.staging_location = 'gs://your-temp-bucket/staging'
    google_cloud_options.temp_location = 'gs://your-temp-bucket/temp'
    google_cloud_options.region = 'us-central1'

    # Create the Pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            # STEP 1: READ
            | "ReadCSV" >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
            
            # STEP 2: PARSE (Transform String -> Dict)
            | "ParseCSV" >> beam.Map(parse_csv_line)
            
            # STEP 3: WRITE
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                TABLE_SPEC,
                # WRITE_TRUNCATE = Replace table content
                # WRITE_APPEND = Add to end
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    print("🚀 Launching Barebones Pipeline...")
    run()
    print("✅ Job submitted to Dataflow!")