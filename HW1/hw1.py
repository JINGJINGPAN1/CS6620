import json
import os
import time

import boto3
from botocore.exceptions import ClientError

# Config
TARGET_USER_NAME = "hw1_user"
BUCKET_PREFIX = "hw1-bucket"
STATE_FILE = "hw1_state.json"

# Initialize clients
iam = boto3.client("iam")
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]

trust_relationship = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
            "Action": "sts:AssumeRole",
        }
    ],
}


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    print(f"State saved to {STATE_FILE}")


def create_role_if_not_exists(role_name, policy_arn):
    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_relationship),
        )
        print(f"Created role: {role_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"Role {role_name} already exists")
        else:
            raise

    iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)


def create_user_if_not_exists(user_name):
    try:
        iam.create_user(UserName=user_name)
        print(f"Created user: {user_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"User {user_name} already exists")
        else:
            raise


def get_or_create_access_key(user_name, state):
    # Check if we have saved keys
    if "AccessKeyId" in state and "SecretAccessKey" in state:
        print(f"Using existing access key: {state['AccessKeyId'][:8]}...")
        return {
            "AccessKeyId": state["AccessKeyId"],
            "SecretAccessKey": state["SecretAccessKey"],
        }

    # Delete old keys to avoid hitting the 2-key limit
    existing = iam.list_access_keys(UserName=user_name).get("AccessKeyMetadata", [])
    for k in existing:
        iam.delete_access_key(UserName=user_name, AccessKeyId=k["AccessKeyId"])
        print(f"Deleted old key: {k['AccessKeyId'][:8]}...")

    # Create new key
    resp = iam.create_access_key(UserName=user_name)
    keys = {
        "AccessKeyId": resp["AccessKey"]["AccessKeyId"],
        "SecretAccessKey": resp["AccessKey"]["SecretAccessKey"],
    }
    print(f"Created new access key: {keys['AccessKeyId'][:8]}...")
    return keys


def add_assume_role_permission(user_name):
    iam.put_user_policy(
        UserName=user_name,
        PolicyName="AllowAssumeRole",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "sts:AssumeRole",
                        "Resource": "*",
                    }
                ],
            }
        ),
    )
    print(f"Added AssumeRole policy to {user_name}")


def assume_role_with_keys(access_key, secret_key, role_arn, session_name="Session"):
    user_sts = boto3.client(
        "sts", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    role_name = role_arn.split("/")[-1]
    print(f"Assuming role: {role_name}...")
    response = user_sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    print(f"Successfully assumed role: {role_name}")
    return response["Credentials"]


def create_s3_resource(credentials):
    return boto3.resource(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_bucket_name():
    return f"{BUCKET_PREFIX}-{account_id[-8:]}"


def create_bucket_if_not_exists(s3_resource, bucket_name):
    bucket = s3_resource.Bucket(bucket_name)
    try:
        s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except ClientError:
        print(f"Creating bucket: {bucket_name}...")
        bucket.create()
        print(f"Bucket created: {bucket_name}")
    return bucket


def upload_files(bucket, files_to_upload):
    print(f"\nUploading {len(files_to_upload)} files...")
    for key, content, upload_type in files_to_upload:
        try:
            if upload_type == "text":
                bucket.put_object(Key=key, Body=content)
            elif upload_type == "file":
                try:
                    bucket.upload_file(content, key)
                except FileNotFoundError:
                    print(f"Warning: file '{content}' not found, skipping")
                    continue
            print(f"Uploaded: {key}")
        except Exception as e:
            print(f"Failed to upload {key}: {e}")
    print("Upload complete")


def list_files_with_prefix(bucket, prefix):
    total_size = 0
    count = 0
    for obj in bucket.objects.filter(Prefix=prefix):
        print(f"  {obj.key} ({obj.size} bytes)")
        total_size += obj.size
        count += 1
    return count, total_size


if __name__ == "__main__":
    state = load_state()

    print("Creating roles and user...")
    create_role_if_not_exists("Dev", "arn:aws:iam::aws:policy/AmazonS3FullAccess")
    create_role_if_not_exists("User", "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    create_user_if_not_exists(TARGET_USER_NAME)

    print("Getting access key...")
    keys = get_or_create_access_key(TARGET_USER_NAME, state)
    state["AccessKeyId"] = keys["AccessKeyId"]
    state["SecretAccessKey"] = keys["SecretAccessKey"]

    print("Adding assume role permission...")
    add_assume_role_permission(TARGET_USER_NAME)
    save_state(state)

    print("Waiting 10 seconds for IAM propagation...")
    time.sleep(10)

    print("Creating bucket and uploading files (Dev role)...")
    dev_role_arn = f"arn:aws:iam::{account_id}:role/Dev"
    dev_creds = assume_role_with_keys(
        keys["AccessKeyId"], keys["SecretAccessKey"], dev_role_arn, "Dev-Session"
    )
    dev_s3 = create_s3_resource(dev_creds)

    bucket_name = get_bucket_name()
    state["BucketName"] = bucket_name
    save_state(state)

    bucket = create_bucket_if_not_exists(dev_s3, bucket_name)
    upload_files(
        bucket,
        [
            ("assignment1.txt", "Empty Assignment 1", "text"),
            ("assignment2.txt", "Empty Assignment 2", "text"),
            ("recording1.jpg", "recording1.jpg", "file"),
        ],
    )

    print("Reading bucket contents (User role - read only)...")
    user_role_arn = f"arn:aws:iam::{account_id}:role/User"
    user_creds = assume_role_with_keys(
        keys["AccessKeyId"], keys["SecretAccessKey"], user_role_arn, "User-Session"
    )
    user_s3 = create_s3_resource(user_creds)
    target_bucket = user_s3.Bucket(bucket_name)

    print(f"\nListing files starting with 'assignment' in {bucket_name}:")
    count, size = list_files_with_prefix(target_bucket, "assignment")
    print(f"\nFound {count} assignment files, total size: {size} bytes")
