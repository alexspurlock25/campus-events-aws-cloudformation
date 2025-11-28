from aws_cdk import (
    Stack, 
    aws_lambda as lambda_, 
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets
)
from constructs import Construct
from lib.config import PipelineConfig

class LambdaStack(Stack):
    '''
    Lambda stack handle the rss to csv handler
    '''

    rss_function: lambda_.Function

    def __init__(self, scope: Construct, id: str, csv_bucket: s3.Bucket, config: PipelineConfig, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        function = lambda_.Function(
            scope=self,
            id=f"RssFunction-{config.rss_feed.name}",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lib/functions/rss_to_csv"),
            environment={
                "RSS_FEED_URL": config.rss_feed.url,
                "RSS_FEED_NAME": config.rss_feed.name,
                "CSV_BUCKET": csv_bucket.bucket_name,
                "ENVIRONMENT": config.environment
            }
        )

        csv_bucket.grant_write(function)

        rule = events.Rule(
            self, 
            f"RssSchedule={config.rss_feed.name}",
            schedule=events.Schedule.expression(config.rss_feed.schedule_expression)
        )

        rule.add_target(
            targets.LambdaFunction(function)
        )

        self.rss_function = function