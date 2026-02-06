import json
import os

import boto3
from botocore.exceptions import ClientError

# Config
TARGET_USER_NAME = "hw1_user"
STATE_FILE = "hw1_state.json"

# Initialize clients
iam = boto3.client("iam")
sts = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def delete_state():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"Deleted state file {STATE_FILE}")


def remove_role(role_name):
    try:
        policies = iam.list_attached_role_policies(RoleName=role_name).get(
            "AttachedPolicies", []
        )
        for p in policies:
            iam.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])
            print(f"Detached {p['PolicyName']} from {role_name}")

        iam.delete_role(RoleName=role_name)
        print(f"Deleted role: {role_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            print(f"Role {role_name} doesn't exist")
        else:
            raise


def remove_user(user_name):
    try:
        # Remove access keys
        keys = iam.list_access_keys(UserName=user_name).get("AccessKeyMetadata", [])
        for k in keys:
            iam.delete_access_key(UserName=user_name, AccessKeyId=k["AccessKeyId"])
            print(f"Deleted key: {k['AccessKeyId'][:8]}...")

        # Remove inline policies
        policies = iam.list_user_policies(UserName=user_name).get("PolicyNames", [])
        for p in policies:
            iam.delete_user_policy(UserName=user_name, PolicyName=p)
            print(f"Deleted policy: {p}")

        iam.delete_user(UserName=user_name)
        print(f"Deleted user: {user_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            print(f"User {user_name} doesn't exist")
        else:
            raise


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


def delete_bucket_and_contents(bucket):
    print(f"\nDeleting bucket: {bucket.name}...")
    try:
        bucket.objects.all().delete()
        print("Deleted all objects")
        bucket.delete()
        print(f"Deleted bucket: {bucket.name}")
    except ClientError as e:
        if "NoSuchBucket" in str(e):
            print(f"Bucket {bucket.name} doesn't exist")
        else:
            raise


if __name__ == "__main__":
    state = load_state()

    if not state:
        print("No state file found, nothing to clean up")
        exit(0)

    print("\n" + "=" * 50)
    print("Cleaning up resources")
    print("=" * 50)

    bucket_name = state.get("BucketName")
    if bucket_name and "AccessKeyId" in state:
        dev_role_arn = f"arn:aws:iam::{account_id}:role/Dev"
        try:
            dev_creds = assume_role_with_keys(
                state["AccessKeyId"],
                state["SecretAccessKey"],
                dev_role_arn,
                "Cleanup-Session",
            )
            cleanup_s3 = create_s3_resource(dev_creds)
            delete_bucket_and_contents(cleanup_s3.Bucket(bucket_name))
        except ClientError as e:
            print(f"Warning: couldn't assume Dev role: {e}")
            print("Trying direct deletion...")
            try:
                s3 = boto3.resource("s3")
                delete_bucket_and_contents(s3.Bucket(bucket_name))
            except Exception as e2:
                print(f"Failed to delete bucket: {e2}")

    remove_user(TARGET_USER_NAME)
    remove_role("Dev")
    remove_role("User")
    delete_state()

    print("\nCleanup complete")
