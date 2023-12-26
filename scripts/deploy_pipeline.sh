#!/bin/bash

python dataflow/pipeline.py \
 --runner DataflowRunner \
 --project apple-health-data-409011 \
 --staging_location gs://ahd_dataflow/staging \
 --temp_location gs://ahd_dataflow/temp \
 --template_location gs://ahd_dataflow/pipeline_templates/ahd_pipeline \
 --region us-west1 \
 --setup_file dataflow/setup.py