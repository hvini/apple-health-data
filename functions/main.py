from googleapiclient.discovery import build
import datetime
import os

PROJECT_ID = os.environ.get("PROJECT_ID")
REGION = os.environ.get("REGION")
TEMPLATE_LOCATION = os.environ.get("TEMPLATE_LOCATION").rstrip("/")
SUBNETWORK = os.environ.get(
    "SUBNETWORK", f"regions/{REGION}/subnetworks/default")
MACHINE_TYPE = os.environ.get("MACHINE_TYPE", "n1-standard-1")

dataflow = build("dataflow", "v1b3")


def handle_pubsub_message(event, context):

    print(
        f"INPUT (event_id='{context.event_id}', timestamp='{context.timestamp}')"
    )

    if 'data' not in event:
        print(
            f"SKIPPING: no data (event_id='{context.event_id}', timestamp='{context.timestamp}')"
        )
        return

    _try_handle_pubsub_message(event, context)


def _try_handle_pubsub_message(event, context):

    input_file = f"gs://{event['attributes']['bucketId']}/{event['attributes']['objectId']}"
    print(f"This File was just uploaded: {input_file}")

    job_name = f"ingestion_{event['attributes']['bucketId']}_{event['attributes']['objectGeneration']}_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"

    job_parameters = {
        "input": input_file
    }

    environment_parameters = {
        "subnetwork": SUBNETWORK,
        "machine_type": MACHINE_TYPE
    }

    request = dataflow.projects().locations().templates().launch(
        projectId=PROJECT_ID,
        location=REGION,
        gcsPath=TEMPLATE_LOCATION,
        body={
            "jobName": job_name,
            "parameters": job_parameters,
            "environment": environment_parameters,
        },
    )

    response = request.execute()
    job = response["job"]

    print(
        f"PIPELINE LAUNCHED (event_id='{context.event_id}', timestamp='{context.timestamp}, job_name='{job_name}', job_id='{job['id']}')"
    )
