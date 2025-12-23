import argparse
import logging
import datetime
import re
import json
import uuid
import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
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

def generate_bq_schema(contract_columns, governance_standards):
    """
    DYNAMICALLY builds the BQ JSON schema for the Storage Write API.
    FIX: Map all temporal types to STRING to avoid Beam SDK serialization bugs.
    """
    schema_fields = []
    
    # 1. Add Data Columns from Contract
    for col in contract_columns:
        dtype = col['type']
        if dtype in ('DATE', 'TIMESTAMP', 'DATETIME'):
            dtype = 'STRING'
        schema_fields.append({'name': col['name'], 'type': dtype, 'mode': col.get('mode', 'NULLABLE')})
    
    # 2. Add Meta Columns from Governance Standards
    for col_name, rules in governance_standards.items():
        dtype = rules['type']
        if dtype in ('DATE', 'TIMESTAMP', 'DATETIME'):
            dtype = 'STRING'
        schema_fields.append({
            'name': col_name, 
            'type': dtype, 
            'mode': 'REQUIRED' if rules['mandatory'] else 'NULLABLE'
        })
        
    return {'fields': schema_fields}

def check_schema_drift(header_line, contract_columns):
    """
    Ensures the incoming file header matches the Data Contract.
    """
    expected_header = [col['name'].upper() for col in contract_columns]
    file_header = [h.strip().upper() for h in header_line.split(',')]
    if file_header != expected_header:
        raise ValueError(f"SCHEMA DRIFT DETECTED: Expected {expected_header}, found {file_header}")
    logging.info("✅ Schema Drift check passed.")
    return True

class ParseBigQueryErrors(beam.DoFn):
    """
    Captures row-level insertion failures from the BigQuery Storage Write API.
    """
    def process(self, element, job_ts):
        destination = element[0]
        row = element[1]
        error_info = element[2]
        error_str = str(error_info)
        yield {
            'original_payload': json.dumps(row),
            'error_message': f"Table: {destination} | Error: {error_str}",
            'failure_type': 'BQ_STORAGE_WRITE_FAILURE',
            'file_name': row.get('meta_source_filename', 'UNKNOWN'),
            'timestamp': job_ts, # String format
            'dq_status': 'FAIL'
        }

def get_meta_provider_registry(file_name, system_name, row_uuid, batch_id, job_ts):
    """
    DECOUPLED REGISTRY: Logic providers for governance columns.
    Uses ISO Strings for all temporal fields to ensure stable serialization.
    """
    # job_ts is already an ISO string from metadata.py
    return {
        'meta_row_uuid': lambda: row_uuid,
        'meta_batch_id': lambda: batch_id,
        'meta_source_system': lambda: system_name,
        'meta_source_filename': lambda: file_name,
        'meta_source_filedate': lambda: job_ts[:10], # Extract YYYY-MM-DD
        'meta_ingest_timestamp': lambda: job_ts,
        'meta_dq_flag': lambda: 'PASS',
        'meta_created_by': lambda: 'dataflow_ingest',
        'meta_created_date': lambda: job_ts,
        'meta_updated_by': lambda: 'dataflow_ingest',
        'meta_updated_date': lambda: job_ts
    }

def populate_meta_columns(row, file_name, system_name, row_uuid, standards, batch_id, job_ts, dq_status):
    """
    DYNAMICALLY appends meta columns based on the Governance Standards.
    """
    providers = get_meta_provider_registry(file_name, system_name, row_uuid, batch_id, job_ts)

    for col_name in standards.keys():
        if col_name == 'meta_dq_flag':
            row[col_name] = dq_status
        elif col_name in providers:
            row[col_name] = providers[col_name]()
        else:
            row[col_name] = None
    
    return row

def validate_value(value, dtype, col_name):
    """
    Strict type checking against Contract Data Types.
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
        elif dtype in ('INTEGER', 'INT64'):
            return int(value)
        elif dtype in ('FLOAT', 'FLOAT64'):
            return float(value)
        elif dtype == 'STRING':
            return value
    except ValueError:
        raise ValueError(f"Invalid {dtype} for {col_name}: '{value}'")
    return value

class ValidateAndParse(beam.DoFn):
    """
    Core logic: Drift Detection, Validation, and Meta Population.
    """
    def process(self, readable_file, contract_columns, unit, standards, batch_id, job_ts):
        file_path = readable_file.metadata.path
        
        with readable_file.open() as f:
            content = f.read().decode('utf-8')
            if not content:
                return
            lines = content.splitlines()

        # 1. Check Schema Drift
        try:
            check_schema_drift(lines[0], contract_columns)
        except Exception as e:
            yield beam.pvalue.TaggedOutput(TAG_INVALID, {
                'original_payload': lines[0],
                'error_message': str(e),
                'file_name': file_path,
                'timestamp': job_ts,
                'dq_status': 'FAIL',
                'failure_type': 'SCHEMA_DRIFT'
            })
            return 

        expected_col_count = len(contract_columns)
        
        for line in lines:
            if not line or (contract_columns and line.startswith(contract_columns[0]['name'])):
                continue

            try:
                row_values = line.split(',')
                if len(row_values) != expected_col_count:
                    raise ValueError(f"Col count mismatch. Expected {expected_col_count}, got {len(row_values)}")

                row = {field['name']: validate_value(row_values[i], field['type'], field['name']) 
                       for i, field in enumerate(contract_columns)}
                
                row_uuid = str(uuid.uuid4())
                row = populate_meta_columns(row, file_path, unit, row_uuid, standards, batch_id, job_ts, 'PASS')
                
                yield beam.pvalue.TaggedOutput(TAG_VALID, row)

            except Exception as e:
                yield beam.pvalue.TaggedOutput(TAG_INVALID, {
                    'original_payload': line,
                    'error_message': str(e),
                    'file_name': file_path,
                    'timestamp': job_ts,
                    'dq_status': 'FAIL',
                    'failure_type': 'VALIDATION_FAILURE'
                })

class DynamicGCSNaming(beam.io.fileio.FileNaming):
    def __call__(self, window, pane, shard_index, total_shards, compression, destination):
        clean_name = destination.split('/')[-1].replace('.csv', '')
        return f"errors/{clean_name}_bad_{shard_index}.json"

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--domain', default='clinical')
    parser.add_argument('--unit', default='synthea')
    parser.add_argument('--table_name', default='conditions')
    parser.add_argument('--env', default='dev')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # 1. LOAD METADATA
    meta = metadata.get_metadata(known_args.domain, known_args.unit, known_args.table_name, known_args.env)
    infra, orch, contract, gov, ctx = meta['infrastructure'], meta['orchestration'], meta['contract'], meta['governance'], meta['context']
    
    validate_target_compliance(infra['service_project'], infra['dataset'], known_args.table_name, gov)

    # DYNAMICALLY GENERATE SCHEMA
    contract_fields = contract.get('schema', {}).get('columns', [])
    full_bq_schema = generate_bq_schema(contract_fields, gov)

    # Use the ISO string directly
    job_ts = ctx['job_timestamp']

    target_table_ref = f"{infra['service_project']}:{infra['dataset']}.{known_args.table_name}"
    bad_table_ref = f"{infra['service_project']}:{infra['dataset']}.{known_args.table_name}_bad"
    dlq_methods = [m.strip().lower() for m in orch.get('dlq_method', 'bq').split(',')]
    input_path = f"gs://{infra['landing_bucket']}/{re.sub(r'<[^>]+>', '*', orch.get('filename_pattern', ''))}"

    pipeline_options = PipelineOptions(pipeline_args)
    google_opts = pipeline_options.view_as(GoogleCloudOptions)
    google_opts.project, google_opts.region, google_opts.service_account_email = infra['shared_project'], infra['region'], infra['dataflow_sa']
    google_opts.temp_location, google_opts.staging_location = f"gs://{infra['temp_bucket']}/temp", f"gs://{infra['staging_bucket']}/staging"

    worker_opts = pipeline_options.view_as(WorkerOptions)
    worker_opts.subnetwork, worker_opts.use_public_ips = infra['subnetwork'], infra['use_public_ips']

    with beam.Pipeline(options=pipeline_options) as p:
        readable_files = p | 'MatchFiles' >> MatchFiles(input_path) | 'ReadMatches' >> ReadMatches()

        results = (
            readable_files 
            | 'ProcessFile' >> beam.ParDo(
                ValidateAndParse(), contract_fields, known_args.unit, gov, ctx['batch_id'], job_ts
            ).with_outputs(TAG_VALID, TAG_INVALID)
        )

        bq_write_results = (
            results[TAG_VALID] 
            | 'WriteTarget' >> beam.io.WriteToBigQuery(
                target_table_ref,
                schema=full_bq_schema,
                create_disposition='CREATE_NEVER',
                write_disposition='WRITE_APPEND',
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API
            )
        )

        bq_failed_rows = bq_write_results['FailedRows'] | 'ParseBQErrors' >> beam.ParDo(ParseBigQueryErrors(), job_ts)
        
        all_invalid = (
            (results[TAG_INVALID], bq_failed_rows) 
            | 'FlattenErrors' >> beam.Flatten()
        )

        if 'bq' in dlq_methods:
            all_invalid | 'BadToBQ' >> beam.io.WriteToBigQuery(
                bad_table_ref,
                # Fixed: timestamp is now STRING in the hint
                schema='original_payload:STRING,error_message:STRING,file_name:STRING,timestamp:STRING,dq_status:STRING,failure_type:STRING',
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND',
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API
            )
        
        if 'gcs' in dlq_methods:
            (all_invalid | 'ToJSON' >> beam.Map(json.dumps)
                         | 'BadToGCS' >> beam.io.fileio.WriteToFiles(
                             path=f"gs://{infra['landing_bucket']}/errors/",
                             destination=lambda r: json.loads(r)['file_name'],
                             sink=beam.io.fileio.TextSink(),
                             file_naming=DynamicGCSNaming()))

if __name__ == '__main__':
    run()