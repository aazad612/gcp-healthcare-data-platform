import apache_beam as beam
def read_csv(p, path):
    return p | beam.io.ReadFromText(path)
