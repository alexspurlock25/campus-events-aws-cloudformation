"""
Storing CSV files
"""

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3, Duration
from constructs import Construct

from lib.config import PipelineConfig


class S3CSVStack(Stack):
    """
    Stack for CSV storage after parsing RSS data.
    """

    csv_bucket: s3.Bucket

    def __init__(
        self, scope: Construct, id: str, config: PipelineConfig, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        rule = s3.LifecycleRule(
            id="MoveOldEventFiles",
            transitions=[
                # First move to an IA S3 (stays here for 5 days)
                s3.Transition(
                    storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=Duration.days(5),
                ),
                # Then Move to Glacier (stays here for 5 days)
                s3.Transition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=Duration.days(10),
                ),
            ],
            # Delete after 20 days (deletes after 10 days of existing)
            expiration=Duration.days(20),
        )

        self.csv_bucket = s3.Bucket(
            self,
            "CsvBucket",
            bucket_name=config.csv_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            versioned=True,
            lifecycle_rules=[rule],
        )
