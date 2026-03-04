from dataclasses import dataclass
import os

from aws_cdk import Duration, Stack, aws_events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_logs as logs
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as sf_tasks
from aws_cdk.aws_lambda import Code, Function, Runtime
from aws_cdk.aws_s3 import Bucket
from constructs import Construct
from lib.config import RssFeedConfig


@dataclass
class RssPipelineOrchestratorStackProps:
    config: RssFeedConfig
    bronze_bucket: Bucket
    bronze_to_silver_state_machine: sf.StateMachine
    silver_to_dynamo_state_machine: sf.StateMachine


class RssPipelineOrchestratorStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        props: RssPipelineOrchestratorStackProps,
        **kwargs,
    ) -> None:
        super().__init__(
            scope, construct_id, stack_name="CampusEventsPipelineOrchestrator", **kwargs
        )

        lambda_dir = os.path.join("lib", "pipeline", "functions")

        get_rss_function = Function(
            scope=self,
            id="GetRssFeedLambda",
            function_name=f"{construct_id}-lambda-fn",
            runtime=Runtime.PYTHON_3_14,
            handler="rss_to_bronze_fn.handler",
            code=Code.from_asset(path=lambda_dir),
            environment={
                "RSS_FEED_URL": props.config.url,
                "BRONZE_BUCKET_NAME": props.bronze_bucket.bucket_name,
            },
            timeout=Duration.seconds(30),
        )

        props.bronze_bucket.grant_write(get_rss_function)

        get_rss_function_task = sf_tasks.LambdaInvoke(
            scope=self,
            id="GetRssFeedTask",
            lambda_function=get_rss_function,
            integration_pattern=sf.IntegrationPattern.REQUEST_RESPONSE,
            timeout=Duration.seconds(60),
        )

        bronze_to_silver_task = sf_tasks.StepFunctionsStartExecution(
            scope=self,
            id="BronzeToSilverTask",
            state_name=f"{construct_id}-bronze-to-silver-task",
            state_machine=props.bronze_to_silver_state_machine,
            integration_pattern=sf.IntegrationPattern.RUN_JOB,
        )

        silver_to_dynamo_task = sf_tasks.StepFunctionsStartExecution(
            scope=self,
            id="SilverToDynamoTask",
            state_name=f"{construct_id}-silver-to-dynamo-task",
            state_machine=props.silver_to_dynamo_state_machine,
            integration_pattern=sf.IntegrationPattern.RUN_JOB,
        )

        main_chain = get_rss_function_task.next(bronze_to_silver_task).next(
            silver_to_dynamo_task
        )

        state_machine = sf.StateMachine(
            scope=self,
            id="RootStateMachine",
            state_machine_name=f"{construct_id}-state-machine",
            definition=main_chain,
            timeout=Duration.minutes(60),
            tracing_enabled=True,
        )

        rule = aws_events.Rule(
            self,
            "PipelineSchedule",
            schedule=aws_events.Schedule.expression(props.config.schedule_expression),
        )

        rule.add_target(targets.SfnStateMachine(state_machine))
