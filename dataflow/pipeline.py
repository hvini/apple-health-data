from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import storage
import apache_beam as beam
import xmltodict
import logging
import os

logging.basicConfig()

STROKE_STYLE_MAP = {
    '0': 'UnknownStrokeStyle',
    '1': 'MixedStrokeStyle',
    '2': 'FreestyleStrokeStyle',
    '3': 'BackstrokeStrokeStyle',
    '4': 'BreaststrokeStrokeStyle',
    '5': 'ButterflyStrokeStyle',
    '6': 'KickboardStrokeStyle'
}

service_account = 'dataflow/apple-health-data-409011-fdaf8a5d0ed6.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account


class AppleHealthDataPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_value_provider_argument(
            "--input",
            type=str,
            help="The file to read from, e.g. gs://bucket/object",
            default='gs://ahd_storage/exportar.xml'
        )


class InputLocation(beam.DoFn):

    def process(self, input_location):

        yield input_location.get()


class ReadFileContent(beam.DoFn):

    def setup(self):

        self.client = storage.Client()

    def process(self, input_location):

        bucket_name, blob_name = input_location.replace(
            'gs://', '').split('/', 1)
        logging.info(f"Reading file: {blob_name}")
        logging.info(f"From bucket: {bucket_name}")
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        content = blob.download_as_string()
        yield content


def parse_stroke_style(value):
    """Converts the stroke style value to its corresponding string."""

    return STROKE_STYLE_MAP.get(value, 'UnknownStrokeStyle')


def fill_na(record):

    record['WeatherTemperature'] = 0 if not 'WeatherTemperature' in record else record['WeatherTemperature']

    for stroke_style in STROKE_STYLE_MAP.values():

        record[stroke_style] = 0 if not stroke_style in record else record[stroke_style]

    return record


def cleanup(record):

    result_dict = {
        'Duration': record['@duration'],
        'CreationDate': record['@creationDate'],
        'StartDate': record['@startDate'],
        'EndDate': record['@endDate']
    }

    metadata_entry = record['MetadataEntry']
    for entry in metadata_entry:

        key = entry['@key']

        if key == 'HKAverageMETs':

            result_dict['AverageMETs'] = entry['@value'].split(' ')[0]

        if key == 'HKWeatherTemperature':

            result_dict['WeatherTemperature'] = entry['@value'].split(' ')[0]

    workout_statistics = record['WorkoutStatistics']
    for stat in workout_statistics:

        type = stat['@type']

        if type == 'HKQuantityTypeIdentifierDistanceSwimming':

            result_dict['DistanceSwimming'] = stat['@sum']

        if type == 'HKQuantityTypeIdentifierActiveEnergyBurned':

            result_dict['EnergyBurned'] = stat['@sum']

    workout_events = record['WorkoutEvent']
    for event in workout_events:

        type = event['@type']
        if type == 'HKWorkoutEventTypeLap':

            metadata_entry = event['MetadataEntry']
            stroke_style = parse_stroke_style(
                metadata_entry['@value'])
            result_dict[stroke_style] = result_dict.get(
                stroke_style, 0) + 1

    result_dict = fill_na(result_dict)
    return result_dict


def filter_swimming_type(doc):

    for rec in doc['HealthData']['Workout']:

        if rec['@workoutActivityType'] == 'HKWorkoutActivityTypeSwimming':

            yield cleanup(rec)


def execute_pipeline(
        options: PipelineOptions,
        input_location
):

    table_schema = {
        'fields': [
            {'name': 'Duration', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CreationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'StartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'EndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'AverageMETs', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'WeatherTemperature', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'DistanceSwimming', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'EnergyBurned', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'UnknownStrokeStyle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'MixedStrokeStyle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'FreestyleStrokeStyle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'BackstrokeStrokeStyle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'BreaststrokeStrokeStyle',
                'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'ButterflyStrokeStyle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'KickboardStrokeStyle', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ]
    }

    with beam.Pipeline(options=options) as pipeline:

        (pipeline
         | 'Create Input' >> beam.Create([input_location])
         | 'Get Input' >> beam.ParDo(InputLocation())
         | 'Read Input Content' >> beam.ParDo(ReadFileContent())
         | 'Convert to Dict' >> beam.Map(lambda str: xmltodict.parse(str))
         | 'Filter Swimming Type' >> beam.FlatMap(filter_swimming_type)
         | 'Write to BQ' >> beam.io.WriteToBigQuery(
             table='apple_health_data.workouts',
             schema=table_schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


def run():

    pipe_options = PipelineOptions().view_as(AppleHealthDataPipelineOptions)
    pipe_options.view_as(SetupOptions).save_main_session = True
    execute_pipeline(pipe_options, pipe_options.input)


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    run()
