import json
import os

import boto3

logs = boto3.client("logs")
LOG_GROUP = os.environ.get("LOG_GROUP_NAME", None)  # leave None to use default


def get_creation_size(object_name, log_group):
    """Search logs for the creation event of this object to find its size."""
    client = boto3.client("logs")
    response = client.filter_log_events(
        logGroupName=log_group,
        filterPattern=f'{{$.object_name = "{object_name}" && $.size_delta > 0}}',
    )
    events = response.get("events", [])
    if events:
        last = json.loads(events[-1]["message"])
        return last["size_delta"]
    return 0


def handler(event, context):
    log_group = LOG_GROUP or context.log_group_name

    for sqs_record in event["Records"]:
        sns_msg = json.loads(sqs_record["body"])
        s3_event = json.loads(sns_msg["Message"])

        for record in s3_event.get("Records", []):
            event_name = record["eventName"]  # e.g. "ObjectCreated:Put"
            object_name = record["s3"]["object"]["key"]
            size = record["s3"]["object"].get("size", None)

            if "ObjectCreated" in event_name:
                size_delta = size
            elif "ObjectRemoved" in event_name:
                # S3 delete events don't include size — look it up from logs
                found_size = get_creation_size(object_name, log_group)
                size_delta = -found_size
            else:
                continue

            print(json.dumps({"object_name": object_name, "size_delta": size_delta}))
