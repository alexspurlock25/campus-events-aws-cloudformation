from dataclasses import dataclass
from typing import Any

from aws_cdk import aws_apigateway, aws_lambda, aws_dynamodb, Stack, Duration
from constructs import Construct


@dataclass
class ApiStackProps:
    dynamodb_table: aws_dynamodb.Table


class ApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        props: ApiStackProps,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        handler = aws_lambda.Function(
            scope=self,
            id="UCEventsAPIHandler",
            runtime=aws_lambda.Runtime.PYTHON_3_14,
            handler="api_get_event_names.lambda_handler",
            code=aws_lambda.Code.from_asset("lib/pipeline/functions"),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": props.dynamodb_table.table_name,
            },
        )

        props.dynamodb_table.grant_read_data(handler)

        api = aws_apigateway.RestApi(
            scope=self,
            id="UCEventsAPI",
            rest_api_name="UC Events Service",
            description="This service serves as the API Gateway for the UC Events Service.",
        )

        events = api.root.add_resource("events")
        events.add_method(
            "GET", aws_apigateway.LambdaIntegration(handler)
        )  # GET /events
