import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.filesystems import FileSystems
import pyarrow.parquet as pq
import pyarrow as pa

PARQUET = 'gs://nombre bucket/*.parquet'
TABLA_BIGQUERY = 'project id:nombre datre lake.nombre tabla a usar'
PROJECT_ID = 'project id'
REGION = 'us-central1'
STAGING_BUCKET = 'gs://nombre bucket/staging'
TEMP_BUCKET = 'gs://nombre bucket/temp'

SCHEMA = {
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
        {'name': 'Airport_fee', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'VendorDesc', 'type': 'STRING', 'mode': 'NULLABLE'}
    ]
}

class LeerParquet(beam.DoFn):
    def process(self, element):
        with FileSystems().open(element) as f:
            parquet_file = pq.ParquetFile(f)
            for batch in parquet_file.iter_batches():
                table = pa.Table.from_batches([batch])
                for row in table.to_pylist():
                    yield row

class FiltrarFilas(beam.DoFn):
    def process(self, row):
        if any(value is None for value in row.values()):
            return
        if 'trip_distance' in row and row['trip_distance'] <= 0:
            return
        for value in row.values():
            if isinstance(value, (int, float)) and value < 0:
                return
        yield row

class AgregarDescripcionVendorID(beam.DoFn):
    VENDOR_MAP = {
        1: "Creative Mobile Technologies, LLC",
        2: "Curb Mobility, LLC",
        6: "Myle Technologies Inc",
        7: "Helix"
    }

    def process(self, row):
        vendor_id = row.get("VendorID")
        row["VendorDesc"] = self.VENDOR_MAP.get(vendor_id, "Desconocido")
        yield row

def run():
    matched_files = FileSystems.match([PARQUET])[0].metadata_list
    file_paths = [f.path for f in matched_files]

    options = PipelineOptions([
        f'--runner=DataflowRunner',
        f'--project={PROJECT_ID}',
        f'--region={REGION}',
        f'--job_name=nyc-parquet-to-bq-cleaned',
        f'--staging_location={STAGING_BUCKET}',
        f'--temp_location={TEMP_BUCKET}',
        '--save_main_session'
    ])

    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Lista de archivos Parquet' >> beam.Create(file_paths)
            | 'Leer Parquet' >> beam.ParDo(LeerParquet())
            | 'Filtrar rows inválidas' >> beam.ParDo(FiltrarFilas())
            | 'Agregar VendorDesc' >> beam.ParDo(AgregarDescripcionVendorID())
            | 'Escribir a BigQuery' >> WriteToBigQuery(
                table=TABLA_BIGQUERY,
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=TEMP_BUCKET
            )
        )

if __name__ == '__main__':
    run()
