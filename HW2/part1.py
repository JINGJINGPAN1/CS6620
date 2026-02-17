import boto3
from botocore.exceptions import ClientError

REGION = "us-west-2"
BUCKET_NAME = "cs6620-hw2-testbucket"
TABLE_NAME = "S3-object-size-history"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


def create_bucket():
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        print(f"S3 bucket '{BUCKET_NAME}' created.")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"S3 bucket '{BUCKET_NAME}' already exists – skipping.")
        else:
            raise


def create_table():
    try:
        ddb.create_table(
            TableName=TABLE_NAME,
            # --- Key schema ---
            # bucket_name  : partition key  → lets us query per-bucket efficiently
            # timestamp    : sort key       → lets us range-query by time
            AttributeDefinitions=[
                {"AttributeName": "bucket_name", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "bucket_name", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",  # on-demand; no capacity planning needed
        )
        waiter = ddb.get_waiter("table_exists")
        waiter.wait(TableName=TABLE_NAME)
        print(f"DynamoDB table '{TABLE_NAME}' created and active.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"DynamoDB table '{TABLE_NAME}' already exists – skipping.")
        else:
            raise


if __name__ == "__main__":
    print(f"Region : {REGION}")
    print(f"Bucket : {BUCKET_NAME}")
    print(f"Table  : {TABLE_NAME}\n")
    create_bucket()
    create_table()
    print("\nDone! Both resources are ready.")
