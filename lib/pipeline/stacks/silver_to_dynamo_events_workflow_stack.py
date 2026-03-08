"""
CDK stack defining the DynamoDB-backed ingestion workflow.
"""

from dataclasses import dataclass

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb,
    aws_glue,
    aws_iam,
    aws_sns,
    aws_sns_subscriptions,
)
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as sf_tasks
from aws_cdk.aws_s3 import IBucket


@dataclass
class SilverToDynamoEventsWorkflowStackProps:
    silver_bucket: IBucket
    scripts_bucket: IBucket
    notification_email: str


class SilverToDynamoEventsWorkflowStack(Stack):
    """
    CDK stack that materializes Silver-layer event data into DynamoDB.
    """

    state_machine: sf.StateMachine
    events_table: aws_dynamodb.Table

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
            stack_name="UCEventsDynamoEventsWfStack",
            **kwargs,
        )

        pipeline_failure_topic = aws_sns.Topic(
            self,
            "WorkflowFailureTopic",
            display_name="UC Events Silver to DynamoDB Failures",
            topic_name=f"{construct_id}-failures",
        )

        pipeline_failure_topic.add_subscription(
            aws_sns_subscriptions.EmailSubscription(props.notification_email)
        )

        pipeline_success_topic = aws_sns.Topic(
            self,
            "WorkflowSuccessTopic",
            display_name="UC Events Silver to DynamoDB Success",
            topic_name=f"{construct_id}-success",
        )

        pipeline_success_topic.add_subscription(
            aws_sns_subscriptions.EmailSubscription(props.notification_email)
        )

        notify_success = sf_tasks.SnsPublish(
            scope=self,
            id="NotifySuccess",
            topic=pipeline_success_topic,
            subject="✅ UC Events Data Pipeline - Silver to DynamoDB Workflow Processing Complete",
            message=sf.TaskInput.from_json_path_at("$"),
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

        dynamo_events_table_name = "uc-events-events-table"
        self.events_table = aws_dynamodb.Table(
            scope=self,
            id="CampusEventsEventsTable",
            table_name=dynamo_events_table_name,
            partition_key=aws_dynamodb.Attribute(
                name="event_id", type=aws_dynamodb.AttributeType.NUMBER
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=False,
        )

        self.events_table.add_global_secondary_index(
            index_name="DateIndex",
            partition_key=aws_dynamodb.Attribute(
                name="start_date", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name="start_time", type=aws_dynamodb.AttributeType.STRING
            ),
        )

        self.events_table.grant_read_write_data(glue_role)

        glue_job_name = f"{construct_id}-job"
        aws_glue.CfnJob(
            scope=self,
            id="SilverToDynamoEventsJob",
            name=glue_job_name,
            role=glue_role.role_arn,
            glue_version="5.1",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=5,
            max_retries=0,
            execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=5
            ),
            command=aws_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{props.scripts_bucket.bucket_name}/glue/silver_to_dynamo_events.py",
            ),
            default_arguments={
                "--extra-py-files": f"s3://{props.scripts_bucket.bucket_name}/uc_types.py",
                "--SILVER_BUCKET_NAME": props.silver_bucket.bucket_name,
                "--DYNAMO_TABLE": dynamo_events_table_name,
            },
        )

        glue_task = sf_tasks.GlueStartJobRun(
            scope=self,
            id="SilverToDynamoDbRunJob",
            glue_job_name=glue_job_name,
            integration_pattern=sf.IntegrationPattern.RUN_JOB,
            timeout=Duration.minutes(10),
        )

        fail_state = sf.Fail(
            scope=self,
            id="InsertIntoDynamoDbFailed",
            cause="Glue job failed",
            error="GlueJobError",
        )

        definition = glue_task.add_catch(
            sf_tasks.SnsPublish(
                scope=self,
                id="NotifyDynamoGlueJobFailure",
                topic=pipeline_failure_topic,
                subject="🚨 UC Events Data Pipeline - Failed to insert into DynamoDB",
                message=sf.TaskInput.from_json_path_at("$.error"),
            ).next(fail_state),
            errors=["States.ALL"],
            result_path="$.error",
        ).next(notify_success)

        self.state_machine = sf.StateMachine(
            scope=self,
            id="SilverToDynamoDbStateMachine",
            state_machine_name=f"{construct_id}-state-machine",
            definition_body=sf.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(5),
        )
