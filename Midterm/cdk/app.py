#!/usr/bin/env python3
import aws_cdk as cdk

from cdk.storage_stack import StorageStack
from cdk.replicator_stack import ReplicatorStack
from cdk.cleaner_stack import CleanerStack

app = cdk.App()

# Stack 1: Storage (S3 buckets + DynamoDB table)
storage = StorageStack(app, "StorageStack")

# Stack 2: Replicator Lambda (depends on storage resources)
ReplicatorStack(
    app, "ReplicatorStack",
    bucket_src=storage.bucket_src,
    bucket_dst=storage.bucket_dst,
    table=storage.table,
)

# Stack 3: Cleaner Lambda (depends on storage resources)
CleanerStack(
    app, "CleanerStack",
    bucket_dst=storage.bucket_dst,
    table=storage.table,
)

app.synth()
