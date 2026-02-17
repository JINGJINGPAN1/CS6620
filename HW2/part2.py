from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
TABLE_NAME = "S3-object-size-history"


def lambda_handler(event, context):
    # Get the bucket name from the S3 event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]

    # Compute total size and object count by listing all objects
    total_size = 0
    object_count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            total_size += obj["Size"]
            object_count += 1

    # Write record to DynamoDB
    timestamp = datetime.now(timezone.utc).isoformat()
    table = ddb.Table(TABLE_NAME)
    table.put_item(
        Item={
            "bucket_name": bucket_name,
            "timestamp": timestamp,
            "total_size": total_size,
            "object_count": object_count,
        }
    )

    print(
        f"Bucket: {bucket_name} | Size: {total_size} bytes | Objects: {object_count} | Time: {timestamp}"
    )
    return {"statusCode": 200, "body": "OK"}
