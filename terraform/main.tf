provider "google" {
  credentials = file("${var.root_dir}/dataflow/apple-health-data-409011-df635517e967.json")
  project     = var.gcp_project
  region      = var.gcp_region
}

resource "google_pubsub_topic" "gcs_topic" {
  name = var.pub_sub_name
}

resource "google_storage_bucket" "gcs_bucket" {
  name          = var.bucket_name
  location      = var.gcp_region
  force_destroy = true
}

resource "google_storage_bucket" "gcs_storage_bucket" {
  name          = var.gcs_storage
  location      = var.gcp_region
  force_destroy = true
}

resource "google_storage_bucket" "gcs_dataflow_bucket" {
  name          = var.gcs_dataflow
  location      = var.gcp_region
  force_destroy = true
}

resource "google_storage_notification" "gcs_notification" {
  bucket = google_storage_bucket.gcs_storage_bucket.name
  topic  = google_pubsub_topic.gcs_topic.name

  payload_format = "JSON_API_V1"
  event_types    = ["OBJECT_FINALIZE"]
}

data "archive_file" "source" {
  type        = "zip"
  source_dir  = "${var.root_dir}/functions"
  output_path = "${var.root_dir}/tmp/function.zip"
}

resource "google_storage_bucket_object" "function" {
  name   = "function.zip"
  bucket = google_storage_bucket.gcs_bucket.name
  source = data.archive_file.source.output_path
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.bq_dataset
  location                   = var.gcp_region
  delete_contents_on_destroy = true
}

resource "null_resource" "dataflow" {
  depends_on = [google_bigquery_dataset.dataset, google_storage_bucket.gcs_dataflow_bucket]
  provisioner "local-exec" {
    command = <<EOT
      source ${var.root_dir}/dataflow/venv/bin/activate && \
      python ${var.root_dir}/dataflow/pipeline.py \
        --runner DataflowRunner \
        --project ${var.gcp_project} \
        --staging_location gs://${var.gcs_dataflow}/staging \
        --temp_location gs://${var.gcs_dataflow}/temp \
        --template_location gs://${var.gcs_dataflow}/${var.template_location} \
        --region ${var.gcp_region} \
        --setup_file ${var.root_dir}/dataflow/setup.py
    EOT
  }
}

resource "google_cloudfunctions2_function" "gcs_function" {
  name        = var.lambda_name
  location    = var.gcp_region
  description = "Cloud Function to process GCS events"

  build_config {
    runtime     = "python311"
    entry_point = var.entry_point

    source {
      storage_source {
        bucket = google_storage_bucket.gcs_bucket.name
        object = google_storage_bucket_object.function.name
      }
    }
  }

  service_config {
    max_instance_count             = 3
    min_instance_count             = 1
    available_memory               = "256M"
    timeout_seconds                = 60
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true

    environment_variables = {
      PROJECT_ID        = var.gcp_project
      REGION            = var.gcp_region
      TEMPLATE_LOCATION = "gs://${var.gcs_dataflow}/${var.template_location}"
    }
  }

  event_trigger {
    trigger_region = var.gcp_region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.gcs_topic.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}
