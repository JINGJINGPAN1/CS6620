import json
import time

import boto3
import urllib3

s3 = boto3.client("s3")

BUCKET_NAME = "cs6620-hw2-testbucket"
PLOTTING_API = (
    "https://udvwuuy6pb.execute-api.us-west-2.amazonaws.com/default/plotting-lambda"
)
SLEEP_SEC = 3  # seconds between operations so dots aren't too close on the plot


def lambda_handler(event, context):
    http = urllib3.PoolManager()

    # 1. Create assignment1.txt  (19 bytes)
    print("Step 1: Creating assignment1.txt...")
    s3.put_object(Bucket=BUCKET_NAME, Key="assignment1.txt", Body="Empty Assignment 1")
    time.sleep(SLEEP_SEC)

    # 2. Update assignment1.txt  (28 bytes)
    print("Step 2: Updating assignment1.txt...")
    s3.put_object(
        Bucket=BUCKET_NAME, Key="assignment1.txt", Body="Empty Assignment 2222222222"
    )
    time.sleep(SLEEP_SEC)

    # 3. Delete assignment1.txt  (0 bytes)
    print("Step 3: Deleting assignment1.txt...")
    s3.delete_object(Bucket=BUCKET_NAME, Key="assignment1.txt")
    time.sleep(SLEEP_SEC)

    # 4. Create assignment2.txt  (2 bytes)
    print("Step 4: Creating assignment2.txt...")
    s3.put_object(Bucket=BUCKET_NAME, Key="assignment2.txt", Body="33")
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
