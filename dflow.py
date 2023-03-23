import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import Map
import csv

# Define pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'priyankachoudhary-1678876060'
google_cloud_options.region = 'asia-south1'
google_cloud_options.job_name = 'csv-to-bigquery'
google_cloud_options.staging_location = 'gs://walmart_pc1/staging/2023-03-15'
google_cloud_options.temp_location = 'gs://walmart_pc1/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

class ParseCSV(beam.DoFn):
  def process(self, element):
    reader = csv.reader([element])
  return next(reader)

# Define BigQuery schema
table_schema = {
  'fields': [
    {'name': 'OrderID', 'type': 'STRING'},
    {'name': 'OrderDate', 'type': 'STRING'},
    {'name': 'ShipDate', 'type': 'STRING'},
    {'name': 'CustomerName', 'type': 'STRING'},
    {'name': 'Country', 'type': 'STRING'},
    {'name': 'City', 'type': 'STRING'},
    {'name': 'State', 'type': 'STRING'},
    {'name': 'Category', 'type': 'STRING'},
    {'name': 'ProductName', 'type': 'STRING'},
    {'name': 'Sales', 'type': 'FLOAT'},
    {'name': 'Quantity', 'type': 'INTEGER'},
    {'name': 'Profit', 'type': 'FLOAT'},
    {'name': 'Total_Sales', 'type': 'FLOAT'}
    ]
  }

# Define pipeline
with beam.Pipeline(options=options) as p:
  # Read the CSV file from GCS
  lines = p | 'Read CSV file from GCS' >> ReadFromText('Walmart.csv')
  line = lines | 'Parse CSV' >> beam.ParDo(ParseCSV())
  # Split each line into columns
  rows = lines | 'Split rows into columns' >> Map(lambda line: line.split(','))
  # Add a new column "Total_Sales"
  data = rows | 'Add Total_Sales column' >> Map(lambda row: {'OrderID': row[0], 'OrderDate': row[1], 'ShipDate': row[2], 'CustomerName': row[3], 'Country': row[4], 'City':row[5], 'State':row[6], 'Category':row[7], 'ProductName':row[8], 'Sales':float(row[9]), 'Quantity':int(row[10]), 'Profit':float(row[11]), 'Total_Sales': float(row[10]) * float(row[11])})
  # Write the data to BigQuery
  data | 'Write data to BigQuery' >> WriteToBigQuery(
    table='priyankachoudhary-1678876060.walmart_data.schema_table',
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
