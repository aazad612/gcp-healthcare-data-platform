import argparse
import logging
import datetime
import re
import json
import uuid
import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from google.cloud import bigquery

# STRICTLY IMPORT YOUR METADATA MODULE
import metadata

# Tags for Branching
TAG_VALID = 'valid'
TAG_INVALID = 'invalid'

def validate_target_compliance(target_project, target_dataset, target_table, standards):
    """
    PRE-FLIGHT CHECK: 
    Compares the actual BigQuery Target Table against the Standards.
    """
    client = bigquery.Client(project=target_project)
    table_ref = f"{target_project}.{target_dataset}.{target_table}"
    try:
        table = client.get_table(table_ref)
    except Exception as e:
        raise ValueError(f"Target table {table_ref} does not exist. Cannot validate standards.") from e

    actual_schema = {field.name: field.field_type for field in table.schema}
    errors = []

    for col_name, rule in standards.items():
        if rule['mandatory']:
            if col_name not in actual_schema:
                errors.append(f"MISSING META COLUMN: '{col_name}' is required by Bronze Standard.")
                continue
            
            actual_type = actual_schema[col_name]
            expected_type = rule['type']
            if actual_type != expected_type:
                errors.append(f"TYPE MISMATCH: '{col_name}' expected {expected_type}, found {actual_type}.")

    if errors:
        error_msg = "\n".join(errors)
        raise ValueError(f"GOVERNANCE COMPLIANCE FAILURE:\n{error_msg}")

    logging.info(f"✅ Target Table {table_ref} is compliant with Bronze Standards.")
    return True

def populate_meta_columns(row, file_name, system_name, row_uuid):
    """
    Appends meta columns to an existing data row.
    """
    now = datetime.datetime.now()
    ts_iso = now.isoformat()
    date_iso = now.date().isoformat()

    row['meta_row_uuid'] = row_uuid
    row['meta_batch_id'] = '0' 
    row['meta_source_system'] = system_name
    row['meta_source_filename'] = file_name
    row['meta_source_filedate'] = date_iso
    row['meta_ingest_timestamp'] = ts_iso
    row['meta_dq_flag'] = 'PASS'
    row['meta_created_by'] = 'dataflow_ingest'
    row['meta_created_date'] = ts_iso
    row['meta_updated_by'] = 'dataflow_ingest'
    row['meta_updated_date'] = ts_iso
    
    return row

def validate_value(value, dtype, col_name):
    """
    Validates a single value against the Contract Data Type.
    """
    value = value.strip()
    dtype = dtype.upper()
    if not value:
        return None

    try:
        if dtype == 'DATE':
            datetime.datetime.strptime(value, '%Y-%m-%d')
            return value
        elif dtype in ('TIMESTAMP', 'DATETIME'):
            datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
            return value
        elif dtype in ('INTEGER', 'INT64', 'INT'):
            return int(value)
        elif dtype in ('FLOAT', 'FLOAT64', 'NUMERIC'):
            return float(value)
        elif dtype == 'STRING':
            return value
    except ValueError:
        raise ValueError(f"Invalid {dtype} for {col_name}: '{value}'")
    return value

class ParseBigQueryErrors(beam.DoFn):
    def process(self, element):
        destination = element[0]
        row = element[1]
        error_info = element[2]
        error_str = str(error_info)
        yield {
            'original_payload': json.dumps(row),
            'error_message': f"Table: {destination} | Error: {error_str}",
            'failure_type': 'BQ_INSERT_FAILURE',
            'file_name': row.get('meta_source_filename', 'UNKNOWN'),
            'timestamp': datetime.datetime.now().isoformat()
        }

def check_schema_drift(header_line, contract_columns):
    expected_header = [col['name'].upper() for col in contract_columns]
    file_header = [h.strip().upper() for h in header_line.split(',')]
    if file_header != expected_header:
        raise ValueError(f"SCHEMA DRIFT: Expected {expected_header}, found {file_header}")
    return True

class ValidateAndParse(beam.DoFn):
    """
    Validates against Contract AND populates Governance Meta Columns.
    """
    def process(self, readable_file, contract_columns, unit):
        file_path = readable_file.metadata.path
        
        with readable_file.open() as f:
            lines = f.read().decode('utf-8').splitlines()

        expected_col_count = len(contract_columns)
        
        for line in lines:
            if contract_columns and line.startswith(contract_columns[0]['name']):
                continue

            try:
                row_values = line.split(',')
                if len(row_values) != expected_col_count:
                    raise ValueError(f"Col count mismatch. Expected {expected_col_count}, got {len(row_values)}")

                # 1. Map and Validate against Contract
                row = {}
                for i, field in enumerate(contract_columns):
                    val = validate_value(row_values[i], field['type'], field['name'])
                    row[field['name']] = val
                
                # 2. Populate Governance Meta Columns
                row_uuid = str(uuid.uuid4())
                row = populate_meta_columns(row, file_path, unit, row_uuid)
                
                yield beam.pvalue.TaggedOutput(TAG_VALID, row)

            except Exception as e:
                yield beam.pvalue.TaggedOutput(TAG_INVALID, {
                    'original_payload': line,
                    'error_message': str(e),
                    'file_name': file_path,
                    'timestamp': datetime.datetime.now().isoformat()
                })

class DynamicGCSNaming(beam.io.fileio.FileNaming):
    def __call__(self, window, pane, shard_index, total_shards, compression, destination):
        base_path = destination
        if '.' in base_path:
            return re.sub(r'(\.[^.]+)$', r'_bad\1', base_path)
        return base_path + '_bad'

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--domain', required=True)
    parser.add_argument('--unit', required=True)
    parser.add_argument('--table_name', required=True)
    parser.add_argument('--env', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # 1. LOAD METADATA
    meta = metadata.get_metadata(
        domain=known_args.domain,
        unit=known_args.unit,
        table_name=known_args.table_name,
        env=known_args.env
    )

    infra = meta['infrastructure']
    orch = meta['orchestration']
    contract = meta['contract']
    governance = meta['governance']  # The Bronze standards
    
    # 2. PRE-FLIGHT GOVERNANCE CHECK
    validate_target_compliance(
        target_project=infra['service_project'],
        target_dataset=infra['dataset'],
        target_table=known_args.table_name,
        standards=governance
    )

    # 3. SETUP PIPELINE CONFIG
    project_id = infra['service_project']
    dataset_id = infra['dataset']
    target_table_ref = f"{project_id}:{dataset_id}.{known_args.table_name}"
    bad_table_ref = f"{project_id}:bronze.{known_args.table_name}_bad"
    dlq_method = orch.get('dlq_method', 'bq') 
    input_pattern = orch.get('source_file_pattern')

    contract_fields = contract.get('columns', [])
    if not contract_fields:
        contract_fields = contract if isinstance(contract, list) else []

    pipeline_options = PipelineOptions(pipeline_args)
    google_opts = pipeline_options.view_as(GoogleCloudOptions)
    google_opts.project = infra['shared_project'] 
    google_opts.temp_location = f"gs://{infra['temp_bucket']}/temp"
    google_opts.staging_location = f"gs://{infra['staging_bucket']}/staging"
    google_opts.service_account_email = infra['dataflow_sa']
    google_opts.subnetwork = infra['subnetwork']
    google_opts.use_public_ips = infra['use_public_ips']
    google_opts.region = infra['region']

    bad_table_schema = {
        'fields': [
            {'name': 'original_payload', 'type': 'STRING'},
            {'name': 'error_message', 'type': 'STRING'},
            {'name': 'file_name', 'type': 'STRING'},
            {'name': 'timestamp', 'type': 'TIMESTAMP'}
        ]
    }

    # 4. BUILD PIPELINE
    with beam.Pipeline(options=pipeline_options) as p:
        readable_files = (
            p 
            | 'MatchFiles' >> MatchFiles(input_pattern)
            | 'ReadMatches' >> ReadMatches()
        )

        results = (
            readable_files 
            | 'ValidateAndMeta' >> beam.ParDo(ValidateAndParse(), contract_fields, known_args.unit)
                                    .with_outputs(TAG_VALID, TAG_INVALID)
        )

        (results[TAG_VALID] 
         | 'WriteTarget' >> beam.io.WriteToBigQuery(
                target_table_ref,
                # Note: valid_bq_schema is omitted to allow BQ to use existing table schema 
                # which now includes your meta columns.
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        invalid_data = results[TAG_INVALID]

        if dlq_method == 'bq':
            (invalid_data
             | 'WriteBadBQ' >> beam.io.WriteToBigQuery(
                    bad_table_ref,
                    schema=bad_table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
        elif dlq_method == 'GCS':
            (invalid_data
             | 'WriteBadGCS' >> beam.io.fileio.WriteToFiles(
                    path='gs://dummy/unused',
                    destination=lambda record: record['file_name'],
                    sink=beam.io.fileio.TextSink(),
                    file_naming=DynamicGCSNaming()
                )
             )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()