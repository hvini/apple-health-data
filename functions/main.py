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
