import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import pyarrow.parquet as pq
import pyarrow as pa
from apache_beam.io.gcp.bigquery import WriteToBigQuery

GCS_FILE_PATH = 'gs://your-bucket/path/to/nyc_yellow_taxi.parquet'
BQ_TABLE = 'your-project-id:data_lake_dataset.nyc_yellow_taxi_raw'

class ReadParquetFromGCS(beam.DoFn):
    def process(self, element):
        from apache_beam.io.filesystems import FileSystems
        with FileSystems().open(element) as f:
            parquet_file = pq.ParquetFile(f)
            for batch in parquet_file.iter_batches():
                table = pa.Table.from_batches([batch])
                for row in table.to_pylist():
                    yield row

def run():
    options = PipelineOptions()

    # Set Google Cloud options
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = 'your-project-id'
    gcp_options.region = 'us-central1'
    gcp_options.job_name = 'nyc-parquet-to-bq-datalake'
    gcp_options.staging_location = 'gs://your-bucket/staging'
    gcp_options.temp_location = 'gs://your-bucket/temp'
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(GoogleCloudOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Create input' >> beam.Create([GCS_FILE_PATH])
            | 'Read Parquet' >> beam.ParDo(ReadParquetFromGCS())
            | 'Write to BigQuery (raw)' >> WriteToBigQuery(
                BQ_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location='gs://your-bucket/temp',
            )
        )

if __name__ == '__main__':
    run()