from aws_cdk import Stack
from aws_cdk import aws_lambda as _lambda
from constructs import Construct


class CampusEventsCloudformationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        fetch_rss_feed_fn = _lambda.Function(
            self,
            "UpdateDataFromRSSFeedFn",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="fetch_new_events.lambda_handler",
            code=_lambda.Code.from_asset("lib"),
        )
