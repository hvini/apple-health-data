from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import pipeline

if __name__ == '__main__':
    options = PipelineOptions()
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DirectRunner'

    input_location = "/Users/vinicius/Projects/hvini/apple-health-data/functions/exportar.xml"
    pipeline.execute_pipeline(options, input_location)
