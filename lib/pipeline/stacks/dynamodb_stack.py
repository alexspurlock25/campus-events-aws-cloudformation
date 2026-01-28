"""
Docstring here
"""

import os
from dataclasses import dataclass

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb,
    aws_lambda,
    aws_s3,
    aws_s3_notifications,
)
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk.aws_lambda import Code, Runtime
from aws_cdk.aws_s3 import IBucket


@dataclass
class DynamoDBStackParamProps:
    staging_bucket: IBucket


class DynamoDBStack(Stack):
    """
    Docstring here
    """

    def __init__(
        self, scope, construct_id, props: DynamoDBStackParamProps, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        events_table = aws_dynamodb.Table(
            scope=self,
            id="CampusEventsEventsTable",
            table_name=f"{construct_id}-table",
            partition_key=aws_dynamodb.Attribute(
                name="event_id", type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=False,
        )

        events_table.add_global_secondary_index(
            index_name="DateIndex",
            partition_key=aws_dynamodb.Attribute(
                name="start_date", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name="start_time", type=aws_dynamodb.AttributeType.STRING
            ),
        )

        lambda_dir = os.path.join("lib", "functions", "csv_to_dynamo_table")

        loader_lambda = aws_lambda.Function(
            scope=self,
            id="CampusEventsCsvLambdaLoader",
            function_name=f"{construct_id}-csv-to-table",
            runtime=Runtime.PYTHON_3_14,
            code=Code.from_asset(path=lambda_dir),
            handler="csv_to_dynamo_table_fn.handler",
            timeout=Duration.minutes(5),
            memory_size=256,  # Sufficient for CSV parsing
            environment={
                "TABLE_NAME": events_table.table_name,
            },
        )
        events_table.grant_write_data(loader_lambda)

        # Create EventBridge rule
        rule = events.Rule(
            scope=self,
            id="CsvUploadInStagingBucketRule",
            rule_name=f"{construct_id}-on-staging-obj-created",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [props.staging_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".csv"}]},
                },
            ),
        )

        # Add Lambda as target
        rule.add_target(targets.LambdaFunction(loader_lambda))

        # Grant Lambda read permissions on the bucket
        props.staging_bucket.grant_read(loader_lambda)
