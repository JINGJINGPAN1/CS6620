import os
import boto3
from datetime import datetime, timezone, timedelta

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

BUCKET_DST = os.environ["BUCKET_DST"]
TABLE_NAME = os.environ["TABLE_NAME"]
GSI_NAME = "status-disowned_at-index"
DISOWN_GRACE_SECONDS = 10


def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    # Threshold: disowned more than 10 seconds ago
    threshold = (datetime.now(timezone.utc) - timedelta(seconds=DISOWN_GRACE_SECONDS)).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    # Query GSI: status = DISOWNED AND disowned_at < threshold
    # No scan needed — uses GSI PK=status, SK=disowned_at
    response = table.query(
        IndexName=GSI_NAME,
        KeyConditionExpression=(
            boto3.dynamodb.conditions.Key("status").eq("DISOWNED")
            & boto3.dynamodb.conditions.Key("disowned_at").lt(threshold)
        ),
    )

    items = response["Items"]
    print(f"Found {len(items)} disowned copies to clean up (threshold={threshold})")

    for item in items:
        copy_key = item["copy_key"]
        original_key = item["original_key"]

        # Delete copy from Bucket Dst
        try:
            s3.delete_object(Bucket=BUCKET_DST, Key=copy_key)
            print(f"Deleted from dst: {copy_key}")
        except Exception as e:
            print(f"Warning: could not delete {copy_key}: {e}")

        # Update Table T: mark as DELETED so it won't appear in future queries
        table.update_item(
            Key={
                "original_key": original_key,
                "copy_key": copy_key,
            },
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "DELETED"},
        )

    print(f"Cleaner done: {len(items)} copies removed")
