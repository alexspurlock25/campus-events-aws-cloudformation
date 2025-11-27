from typing import Any, Dict, Mapping, Sequence
from aws_cdk import Stack, aws_lambda, aws_s3
from constructs import Construct
from lib.config import PipelineConfig

class LambdaStack(Stack):
    '''
    Lambda stack handle the rss to csv handler
    '''

    def __init__(self, scope: Construct, id: str, csv_bucket: aws_s3.Bucket, config: PipelineConfig, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        function = aws_lambda.Function(
            scope=self,
            id=f"RssFunction-{config.rss_feed.name}",
            runtime=aws_lambda.Runtime.PYTHON_3_14,
            handler="handler.lambda_handler",
            code=aws_lambda.Code.from_asset("lib/functions/rss_to_csv"),
            environment={
                "RSS_FEED_URL": config.rss_feed.url,
                "RSS_FEED_NAME": config.rss_feed.name,
                "CSV_BUCKET": csv_bucket.bucket_name,
                "ENVIRONMENT": config.environment
            }
        )