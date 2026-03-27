import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Source bucket
        self.bucket_src = s3.Bucket(
            self, "BucketSrc",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,
            event_bridge_enabled=True,  # Enable EventBridge so Replicator can receive S3 events
        )

        # Destination bucket
        self.bucket_dst = s3.Bucket(
            self, "BucketDst",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # DynamoDB Table T
        # PK: original_key (e.g. "Assignment1.txt")
        # SK: copy_key     (e.g. "Assignment1.txt/20240101T000000Z")
        # GSI: status (PK) + disowned_at (SK) — used by Cleaner, no scan needed
        self.table = dynamodb.Table(
            self, "TableT",
            partition_key=dynamodb.Attribute(
                name="original_key",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="copy_key",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # GSI: status + disowned_at
        self.table.add_global_secondary_index(
            index_name="status-disowned_at-index",
            partition_key=dynamodb.Attribute(
                name="status",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="disowned_at",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # Outputs
        CfnOutput(self, "BucketSrcName", value=self.bucket_src.bucket_name)
        CfnOutput(self, "BucketDstName", value=self.bucket_dst.bucket_name)
        CfnOutput(self, "TableName", value=self.table.table_name)
