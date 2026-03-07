import boto3
import os
from datetime import datetime, timezone

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


def handler(event, context):
    # get the bucket_name and table_name from environment variables
    bucket_name = os.environ['BUCKET_NAME']
    table_name = os.environ['TABLE_NAME']

    # list all objects in the bucket and calculate total size and object count
    total_size = 0
    object_count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            total_size += obj["Size"]
            object_count += 1

    # get the current timestamp in ISO format
    timestamp = datetime.now(timezone.utc).isoformat()

    # write the record to DynamoDB
    table = dynamodb.Table(table_name)
    table.put_item(
        Item={
            "bucket_name": bucket_name,
            "timestamp": timestamp,
            "total_size": total_size,
            "object_count": object_count,
        }
    )