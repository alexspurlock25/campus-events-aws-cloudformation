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

    landing_bucket: s3.Bucket
    staging_bucket: s3.Bucket

    def __init__(
        self, scope: Construct, construct_id: str, config: PipelineConfig, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        rule = s3.LifecycleRule(
            id=f"{construct_id}-mv-rule",
            transitions=[
                # First move to an IA S3 (stays here for 30 days)
                s3.Transition(
                    storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=Duration.days(30),
                ),
                # Then Move to Glacier (stays here for 30 days)
                s3.Transition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=Duration.days(60),
                ),
            ],
            # Delete after 90 days from creation
            expiration=Duration.days(90),
        )

        self.landing_bucket = s3.Bucket(
            scope=self,
            id=f"{construct_id}-{config.landing_suffix}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[rule],
            # Encryption enabled by default by AWS on the server side.
            # I am doing explict work for learning purposes.
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        self.staging_bucket = s3.Bucket(
            scope=self,
            id=f"{construct_id}-{config.staging_suffix}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[rule],
            # Encryption enabled by default by AWS on the server side.
            # I am doing explict work for learning purposes.
            encryption=s3.BucketEncryption.S3_MANAGED,
        )
