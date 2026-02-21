import os
from dataclasses import dataclass

from aws_cdk import Duration, Stack
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk.aws_lambda import Code, Function, Runtime
from constructs import Construct

from lib.config import RssFeedConfig


@dataclass
class GetRssLambdaStackProps:
    bronze_bucket: s3.Bucket
    config: RssFeedConfig


class GetRssLambdaStack(Stack):
    """
    Lambda stack to manage resource of fetching RSS
    data and putting that data into data lake bronze zone
    """

    get_rss_function: lambda_.Function

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        props: GetRssLambdaStackProps,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            stack_name="CampusEventsGetRssFeedLambda",
            **kwargs,
        )

        lambda_dir = os.path.join("lib", "pipeline", "functions")

        self.get_rss_function = Function(
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

        props.bronze_bucket.grant_write(self.get_rss_function)
