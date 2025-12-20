"""
Docstring here
"""

from aws_cdk import RemovalPolicy, Stack, aws_dynamodb


class DynamoDBStack(Stack):
    """
    Docstring here
    """

    def __init__(self, scope, construct_id) -> None:
        super().__init__(scope, construct_id)

        events_table = aws_dynamodb.Table(
            scope=self,
            id="CampusEventsEventsTable",
            table_name=f"{construct_id}-table",
            partition_key=aws_dynamodb.Attribute(
                name="event_id", type=aws_dynamodb.AttributeType.STRING
            ),
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
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
