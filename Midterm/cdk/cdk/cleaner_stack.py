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


class CleanerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket_dst: s3.IBucket,
        table: dynamodb.ITable,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # Cleaner Lambda
        cleaner_fn = lambda_.Function(
            self, "CleanerLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("../lambda/cleaner"),
            timeout=Duration.seconds(60),
            environment={
                "BUCKET_DST": bucket_dst.bucket_name,
                "TABLE_NAME": table.table_name,
            },
        )

        # Grant permissions
        bucket_dst.grant_read_write(cleaner_fn)
        table.grant_read_write_data(cleaner_fn)

        # EventBridge rule: trigger every 1 minute
        rule = events.Rule(
            self, "CleanerSchedule",
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        rule.add_target(targets.LambdaFunction(cleaner_fn))
