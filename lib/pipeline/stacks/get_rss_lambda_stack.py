import os

from aws_cdk import Duration, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk.aws_lambda import Code, Function, Runtime
from constructs import Construct

from lib.config import PipelineConfig


class GetRssLambdaStack(Stack):
    """
    Lambda stack to manage resource of fetching RSS
    data and putting that data into data lake bronze zone
    """

    rss_function: lambda_.Function

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bronze_bucket: s3.Bucket,
        config: PipelineConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, stack_name="GetRssLambda", **kwargs)

        lambda_dir = os.path.join("lib", "pipeline", "functions", "rss_to_bronze")

        function = Function(
            scope=self,
            id="FetchRssFeedFn",
            function_name=f"{construct_id}-fn",
            runtime=Runtime.PYTHON_3_14,
            handler="rss_to_bronze_fn.handler",
            code=Code.from_docker_build(path=lambda_dir, file="Dockerfile"),
            environment={
                "RSS_FEED_URL": config.rss_feed.url,
                "RSS_FEED_NAME": config.rss_feed.name,
                "BRONZE_BUCKET_NAME": bronze_bucket.bucket_name,
            },
            timeout=Duration.seconds(30),
        )

        bronze_bucket.grant_write(function)

        rule = events.Rule(
            scope=self,
            id="CampusEventsLambdaScheduleRule",
            rule_name=f"{construct_id}-schedule-rule",
            schedule=events.Schedule.expression(config.rss_feed.schedule_expression),
        )

        rule.add_target(targets.LambdaFunction(function))

        self.rss_function = function
