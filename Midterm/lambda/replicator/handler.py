import os
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

BUCKET_SRC = os.environ["BUCKET_SRC"]
BUCKET_DST = os.environ["BUCKET_DST"]
TABLE_NAME = os.environ["TABLE_NAME"]

MAX_COPIES = 3


def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)

    # EventBridge S3 event format:
    # event["detail-type"] = "Object Created" or "Object Deleted"
    # event["detail"]["object"]["key"] = object key
    detail_type = event.get("detail-type", "")
    original_key = event.get("detail", {}).get("object", {}).get("key", "")

    if not original_key:
        print("No object key found in event, skipping.")
        return

    if detail_type == "Object Created":
        handle_put(table, original_key)
    elif detail_type == "Object Deleted":
        handle_delete(table, original_key)
    else:
        print(f"Unknown detail-type: {detail_type}, skipping.")


def handle_put(table, original_key):
    # Generate a unique copy key using current UTC timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    copy_key = f"{original_key}/{timestamp}"

    # Copy object to Bucket Dst
    s3.copy_object(
        Bucket=BUCKET_DST,
        CopySource={"Bucket": BUCKET_SRC, "Key": original_key},
        Key=copy_key,
    )

    # Add new record to Table T
    table.put_item(
        Item={
            "original_key": original_key,
            "copy_key": copy_key,
            "created_at": timestamp,
            "status": "ACTIVE",
            "disowned_at": "NONE",  # placeholder so GSI SK always has a value
        }
    )

    # Check if there are now more than MAX_COPIES; if so, delete the oldest
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("original_key").eq(original_key),
    )
    items = response["Items"]

    # Only count ACTIVE copies, sort by created_at ascending (oldest first)
    active_items = [i for i in items if i["status"] == "ACTIVE"]
    active_items.sort(key=lambda x: x["created_at"])

    if len(active_items) > MAX_COPIES:
        oldest = active_items[0]
        try:
            s3.delete_object(Bucket=BUCKET_DST, Key=oldest["copy_key"])
        except Exception as e:
            print(f"Warning: could not delete {oldest['copy_key']} from dst: {e}")
        table.delete_item(
            Key={
                "original_key": oldest["original_key"],
                "copy_key": oldest["copy_key"],
            }
        )
        print(f"Deleted oldest copy: {oldest['copy_key']}")

    print(f"PUT handled: {original_key} -> {copy_key}")


def handle_delete(table, original_key):
    # Query all copies of this original_key
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("original_key").eq(original_key),
    )
    items = response["Items"]

    disowned_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    for item in items:
        if item["status"] == "ACTIVE":
            table.update_item(
                Key={
                    "original_key": item["original_key"],
                    "copy_key": item["copy_key"],
                },
                UpdateExpression="SET #s = :s, disowned_at = :da",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": "DISOWNED",
                    ":da": disowned_at,
                },
            )

    print(f"DELETE handled: {original_key}, {len(items)} copies marked DISOWNED")
