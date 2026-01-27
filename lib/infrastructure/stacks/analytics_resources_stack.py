"""
Analytics infrastructure stack.
Manages S3 bucket for Athena query results and analytics outputs.
"""

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from constructs import Construct


class AnalyticsResourcesStack(Stack):
    """
    Stack for analytics query results.
    Manages S3 bucket for Athena query results with automatic cleanup.
    """

    athena_results_bucket: s3.Bucket

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope, construct_id, stack_name="CampusEventsAnalyticsResources", **kwargs
        )

        self.athena_results_bucket = s3.Bucket(
            scope=self,
            id="AthenaResultsBucket",
            bucket_name=f"{construct_id}-athena-results",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
        )
