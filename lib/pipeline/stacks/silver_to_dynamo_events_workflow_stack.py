"""
CDK stack defining the DynamoDB-backed ingestion workflow.
"""

from dataclasses import dataclass

from aws_cdk import (
    Aws,
    RemovalPolicy,
    Stack,
    aws_dynamodb,
    aws_glue,
    aws_iam,
)
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk.aws_s3 import IBucket


@dataclass
class SilverToDynamoEventsWorkflowStackProps:
    bronze_bucket: IBucket
    silver_bucket: IBucket
    scripts_bucket: IBucket


class SilverToDynamoEventsWorkflowStack(Stack):
    """
    CDK stack that materializes Silver-layer event data into DynamoDB.
    """

    def __init__(
        self,
        scope,
        construct_id,
        props: SilverToDynamoEventsWorkflowStackProps,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            stack_name="CampusEventsDynamoEventsWorkflow",
            **kwargs,
        )

        glue_role = aws_iam.Role(
            scope=self,
            id="SilverToDynamoEventsGlueJobServiceRole",
            role_name=f"{construct_id}-glue-role",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        props.scripts_bucket.grant_read(glue_role)
        props.silver_bucket.grant_read_write(glue_role)

        dynamo_events_table_name = f"{construct_id}-events-table"
        events_table = aws_dynamodb.Table(
            scope=self,
            id="CampusEventsEventsTable",
            table_name=dynamo_events_table_name,
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

        glue_job_name = f"{construct_id}-job"
        job = aws_glue.CfnJob(
            scope=self,
            id="SilverToDynamoEventsJob",
            name=glue_job_name,
            role=glue_role.role_arn,
            glue_version="5.1",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=5,
            max_retries=0,
            command=aws_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{props.scripts_bucket.bucket_name}/glue/silver_to_dynamo_events.py",
            ),
            default_arguments={
                "--continuous-log-logGroup": f"/aws-glue/jobs/{glue_job_name}",
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": f"s3://{props.bronze_bucket.bucket_name}/silver_to_dynamo_events/",
                "--enable-spark-ui": "true",
                "--enable-metrics": "true",
                "--SILVER_BUCKET": props.silver_bucket.bucket_name,
                "--DYNAMO_TABLE": dynamo_events_table_name,
            },
        )

        # Create EventBridge rule
        rule = events.Rule(
            scope=self,
            id="LoadEventsOnSilverArrivalRule",
            rule_name=f"{construct_id}-on-silver-arrival",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [props.silver_bucket.bucket_name]},
                    "object": {
                        "key": [
                            {"prefix": "year="},
                            {"suffix": ".parquet"},
                        ]
                    },
                },
            ),
        )

        rule.add_target(
            targets.AwsApi(
                service="Glue",
                action="startJobRun",
                parameters={"JobName": job.ref},
                policy_statement=aws_iam.PolicyStatement(
                    actions=["glue:StartJobRun"],
                    resources=[
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:job/{glue_job_name}"
                    ],
                ),
            )
        )
