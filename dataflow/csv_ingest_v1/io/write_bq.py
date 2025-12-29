import apache_beam as beam
def write_to_bq(pcoll, table, schema):
    return pcoll | beam.io.WriteToBigQuery(table, schema=schema)
