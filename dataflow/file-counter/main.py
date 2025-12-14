import json
import apache_beam as beam

class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        """Pub/Sub message → extract GCS bucket + filename."""
        message = json.loads(element)
        bucket = message["bucket"]
        name = message["name"]
        yield f"gs://{bucket}/{name}"


class CountLines(beam.DoFn):
    def process(self, file_path):
        """Read file from GCS and count rows."""
        from apache_beam.io.gcp.gcsio import GcsIO

        gcs = GcsIO()
        total = 0
        with gcs.open(file_path) as f:
            for _ in f:
                total += 1

        print(f"[DATAFLOW] File: {file_path}, Row Count = {total}")
        yield {
            "file": file_path,
            "row_count": total
        }


def run():
    from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

    pipeline_options = PipelineOptions()

    # “Serialize everything I defined in my main Python file and send it to workers.”
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    class CustomOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                "--input_subscription",
                required=True,
                help="Pub/Sub subscription to read from"
            )

    opts = pipeline_options.view_as(CustomOptions)
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=opts.input_subscription)
            | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))
            | "ParseMessage" >> beam.ParDo(ParsePubSubMessage())
            | "CountLines" >> beam.ParDo(CountLines())
        )

