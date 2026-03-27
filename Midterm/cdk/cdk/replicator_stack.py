import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
)
from constructs import Construct


class ReplicatorStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket_src: s3.IBucket,
        bucket_dst: s3.IBucket,
        table: dynamodb.ITable,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # Replicator Lambda
        replicator_fn = lambda_.Function(
            self, "ReplicatorLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("../lambda/replicator"),
            timeout=Duration.seconds(30),
            environment={
                "BUCKET_SRC": bucket_src.bucket_name,
                "BUCKET_DST": bucket_dst.bucket_name,
                "TABLE_NAME": table.table_name,
            },
        )

        # Grant permissions
        bucket_src.grant_read(replicator_fn)
        bucket_dst.grant_read_write(replicator_fn)
        table.grant_read_write_data(replicator_fn)

        # Trigger via EventBridge (avoids cross-stack cyclic dependency)
        # BucketSrc has event_bridge_enabled=True in StorageStack
        rule = events.Rule(
            self, "ReplicatorRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created", "Object Deleted"],
                detail={
                    "bucket": {
                        "name": [bucket_src.bucket_name]
                    }
                },
            ),
        )
        rule.add_target(targets.LambdaFunction(replicator_fn))
