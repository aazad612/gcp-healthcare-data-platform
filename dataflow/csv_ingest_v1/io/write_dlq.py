import apache_beam as beam
def write_dlq(pcoll, table):
    return pcoll | beam.io.WriteToBigQuery(table)
