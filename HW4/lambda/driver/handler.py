import os
import time

import boto3
import urllib3

s3 = boto3.client("s3")


def wait_for_size_below_threshold(bucket_name, threshold=20, timeout=180, interval=10):
    """Poll until total bucket size drops below threshold (Cleaner has run)."""
    waited = 0
    while waited < timeout:
        total_size = 0
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                if obj["Key"] != "plot.png":
                    total_size += obj["Size"]

        print(f"Current total size: {total_size} bytes")
        if total_size <= threshold:
            print("Size below threshold, Cleaner has done its job.")
            return True

        time.sleep(interval)
        waited += interval

    print("Timeout: Cleaner did not run in time.")
    return False


def handler(event, context):

    bucket_name = os.environ["BUCKET_NAME"]
    plotting_api = os.environ["PLOTTING_API_URL"]
    http = urllib3.PoolManager()

    # 1. Create assignment1.txt  (18 bytes)
    s3.put_object(Bucket=bucket_name, Key="assignment1.txt", Body="Empty Assignment 1")
    time.sleep(10)

    # 2. Create assignment2.txt (27 bytes) — total: 46 > 20, alarm fires → Cleaner deletes assignment2.txt
    s3.put_object(
        Bucket=bucket_name, Key="assignment2.txt", Body="Empty Assignment 2222222222"
    )
    wait_for_size_below_threshold(bucket_name, threshold=20)

    # 3. assignment3.txt = 3 bytes ("333") → 18+3=21 → second alarm → Cleaner deletes assignment1
    s3.put_object(Bucket=bucket_name, Key="assignment3.txt", Body="333")
    wait_for_size_below_threshold(bucket_name, threshold=20)

    # 4. Call plotting API
    response = http.request("GET", plotting_api)
    print(response.data.decode("utf-8"))

    return {"statusCode": 200, "body": "Driver complete"}
