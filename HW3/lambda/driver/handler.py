import json
import time

import boto3
import urllib3
import os

s3 = boto3.client("s3")
SLEEP_SEC = 2  # seconds between operations so dots aren't too close on the plot


def handler(event, context):

    bucket_name = os.environ['BUCKET_NAME']
    PLOTTING_API = os.environ['PLOTTING_API_URL']

    http = urllib3.PoolManager()

    # 1. Create assignment1.txt  (19 bytes)
    print("Step 1: Creating assignment1.txt...")
    s3.put_object(Bucket=bucket_name, Key="assignment1.txt", Body="Empty Assignment 1")
    time.sleep(SLEEP_SEC)

    # 2. Update assignment1.txt  (28 bytes)
    print("Step 2: Updating assignment1.txt...")
    s3.put_object(
        Bucket=bucket_name, Key="assignment1.txt", Body="Empty Assignment 2222222222"
    )
    time.sleep(SLEEP_SEC)

    # 3. Delete assignment1.txt  (0 bytes)
    print("Step 3: Deleting assignment1.txt...")
    s3.delete_object(Bucket=bucket_name, Key="assignment1.txt")
    time.sleep(SLEEP_SEC)

    # 4. Create assignment2.txt  (2 bytes)
    print("Step 4: Creating assignment2.txt...")
    s3.put_object(Bucket=bucket_name, Key="assignment2.txt", Body="33")
    time.sleep(SLEEP_SEC)

    # 5. Call the plotting lambda API
    print("Step 5: Calling plotting API...")
    response = http.request("GET", PLOTTING_API)
    body = response.data.decode("utf-8")
    print(f"Plotting API response: {body}")

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Driver complete", "plot_response": body}),
    }
