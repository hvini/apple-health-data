#!/bin/bash

gcloud functions deploy handlePubSubMessage --source=./functions --gen2 --runtime=python311 --region=us-west1 --entry-point=handle_pubsub_message --trigger-topic=ahd_topic --set-env-vars=PROJECT_ID=apple-health-data-409011,REGION=us-west1,TEMPLATE_LOCATION=gs://ahd_dataflow/pipeline_templates/ahd_pipeline
