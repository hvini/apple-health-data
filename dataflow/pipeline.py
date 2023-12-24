from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import xml.etree.ElementTree as ET
import apache_beam as beam
import logging

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


def parse_stroke_style(value):
    """Converts the stroke style value to its corresponding string."""

    return STROKE_STYLE_MAP.get(value, 'UnknownStrokeStyle')


class AppleHealthDataPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_value_provider_argument(
            "--input",
            type=str,
            help="The file to read from, e.g. gs://bucket/object",
        )
        parser.add_value_provider_argument(
            '--pipeline_name',
            type=str,
            help='This pipe name',
        )


class ReadXML(beam.DoFn):

    def process(self, file_path):

        tree = ET.parse(file_path)
        root = tree.getroot()

        for workout in root.findall('Workout'):

            if workout.get('workoutActivityType') == 'HKWorkoutActivityTypeSwimming':

                yield ET.tostring(workout)


class ParseXML(beam.DoFn):

    def process(self, str):

        element = ET.fromstring(str)

        result_dict = {
            'Duration': element.attrib.get('duration', ''),
            'CreationDate': element.attrib.get('creationDate', ''),
            'StartDate': element.attrib.get('startDate', ''),
            'EndDate': element.attrib.get('endDate', '')
        }

        for parent in element:

            tag = parent.tag
            attrib_value = parent.attrib.values()

            if tag == 'MetadataEntry':

                if 'HKAverageMETs' in attrib_value:

                    result_dict['AverageMETs'] =\
                        parent.attrib['value'].split(' ')[0]

                if 'HKWeatherTemperature' in attrib_value:

                    result_dict['WeatherTemperature'] = parent.attrib['value']\
                        .split(' ')[0]

            if tag == 'WorkoutStatistics':

                if 'HKQuantityTypeIdentifierDistanceSwimming' in attrib_value:

                    result_dict['DistanceSwimming'] = parent.attrib['sum']

                if 'HKQuantityTypeIdentifierActiveEnergyBurned' in attrib_value:

                    result_dict['EnergyBurned'] = parent.attrib['sum']

            if (tag == 'WorkoutEvent' and 'HKWorkoutEventTypeLap' in parent.attrib.values()):

                for child in parent:

                    stroke_style = parse_stroke_style(child.attrib['value'])
                    result_dict[stroke_style] = result_dict.get(
                        stroke_style, 0) + 1

        yield result_dict


class FillNa(beam.DoFn):

    def process(self, element):

        if not 'WeatherTemperature' in element:
            element['WeatherTemperature'] = 0

        for stroke_style in STROKE_STYLE_MAP.values():

            if not stroke_style in element:
                element[stroke_style] = 0

        yield element


class CombineLines(beam.DoFn):

    def process(self, lines):

        combined_str = ''.join(lines)
        yield combined_str


def execute_pipeline(
        options: AppleHealthDataPipelineOptions,
        input_location
):

    with beam.Pipeline(options=options) as pipeline:

        data = (pipeline
                | 'Create File Path' >> beam.Create([input_location])
                | 'Read XML' >> beam.ParDo(ReadXML())
                | 'Parse XML' >> beam.ParDo(ParseXML())
                | 'Fill NA' >> beam.ParDo(FillNa()))


def run():

    pipe_options = PipelineOptions().view_as(AppleHealthDataPipelineOptions)
    pipe_options.view_as(SetupOptions).save_main_session = True
    logging.info(f"Pipeline: {pipe_options.pipeline_name}")
    execute_pipeline(pipe_options, pipe_options.input)
    logging.info("FINISHED.")


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    run()
