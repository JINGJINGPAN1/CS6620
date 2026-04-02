import os

import boto3

s3 = boto3.client("s3")
# Must match plotting Lambda upload key (see lambda/plotting/handler.py PLOT_KEY).
SKIP_KEYS = frozenset({"plot", "plot.png"})


def handler(event, context):
    bucket_name = os.environ["BUCKET_NAME"]

    # List all objects and find the largest
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            if obj["Key"] not in SKIP_KEYS:
                objects.append(obj)

    if not objects:
        print("Bucket is empty, nothing to delete.")
        return

    largest = max(objects, key=lambda o: o["Size"])
    print(f"Deleting largest object: {largest['Key']} ({largest['Size']} bytes)")
    s3.delete_object(Bucket=bucket_name, Key=largest["Key"])
