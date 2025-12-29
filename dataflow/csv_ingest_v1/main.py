import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from common.metadata_loader import load_runtime_metadata
from pipeline import build_pipeline


def run(input_file: str, env: str | None):
    # ---- Beam options (container only) ----
    beam_options = PipelineOptions(
        save_main_session=True,
        input_file=input_file,
        env=env
    )

    # ---- Resolve ALL metadata here ----
    runtime_metadata = load_runtime_metadata(beam_options)

    # ---- Inject Dataflow execution options ----
    beam_options.view_as(PipelineOptions).runner = runtime_metadata["runner"]
    beam_options.view_as(PipelineOptions).project = runtime_metadata["dataflow_project"]
    beam_options.view_as(PipelineOptions).region = runtime_metadata["region"]

    beam_options.view_as(PipelineOptions).temp_location = runtime_metadata["temp_location"]
    beam_options.view_as(PipelineOptions).staging_location = runtime_metadata["staging_location"]

    beam_options.view_as(PipelineOptions).subnetwork = runtime_metadata["subnetwork"]
    beam_options.view_as(PipelineOptions).use_public_ips = False
    beam_options.view_as(PipelineOptions).save_main_session = True

    # ---- Build + run pipeline ----
    with beam.Pipeline(options=beam_options) as p:
        build_pipeline(p, runtime_metadata)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Metadata-driven Dataflow ingestion job")

    parser.add_argument(
        "--input_file",
        required=True,
        help="GCS URI of file to ingest (passed by Airflow / CF)"
    )

    parser.add_argument(
        "--env",
        required=False,
        help="Environment override (dev/qa/uat/prod). Optional if encoded in path."
    )

    args = parser.parse_args()

    run(
        input_file=args.input_file,
        env=args.env
    )
