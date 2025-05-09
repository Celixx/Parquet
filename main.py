import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import pyarrow.parquet as pq
import pyarrow as pa
from apache_beam.io.filesystems import FileSystems

# Constants
GCS_FILE_PATH = 'gs://your-bucket/path/to/nyc_yellow_taxi.parquet'
BQ_TABLE = 'your-project-id:lake_parquet.nyc_yellow_taxi_raw'
PROJECT_ID = 'your-project-id'
REGION = 'your-region'  # e.g., us-central1
STAGING_BUCKET = 'gs://your-bucket/staging'
TEMP_BUCKET = 'gs://your-bucket/temp'

# Updated BigQuery Schema
BQ_SCHEMA = {
    'fields': [
        {'name': 'VendorID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'tpep_pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'tpep_dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'passenger_count', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'trip_distance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'RatecodeID', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'store_and_fwd_flag', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PULocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'DOLocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'payment_type', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'fare_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'extra', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'mta_tax', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'tip_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'tolls_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'improvement_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'congestion_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'Airport_fee', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
}

class ReadParquetFromGCS(beam.DoFn):
    def process(self, element):
        with FileSystems().open(element) as f:
            parquet_file = pq.ParquetFile(f)
            for batch in parquet_file.iter_batches():
                table = pa.Table.from_batches([batch])
                for row in table.to_pylist():
                    yield row

def run():
    options = PipelineOptions([
        f'--runner=DataflowRunner',
        f'--project={PROJECT_ID}',
        f'--region={REGION}',
        f'--job_name=nyc-parquet-to-bq-datalake',
        f'--staging_location={STAGING_BUCKET}',
        f'--temp_location={TEMP_BUCKET}',
        '--save_main_session'
    ])

    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Create GCS file path' >> beam.Create([GCS_FILE_PATH])
            | 'Read Parquet' >> beam.ParDo(ReadParquetFromGCS())
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=BQ_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=TEMP_BUCKET
            )
        )

if __name__ == '__main__':
    run()
