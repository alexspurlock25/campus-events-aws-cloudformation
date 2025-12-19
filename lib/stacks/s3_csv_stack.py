"""
Storing CSV files
"""

import os

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deploy
from constructs import Construct

from lib.config import PipelineConfig


class S3CSVStack(Stack):
    """
    Stack for CSV storage after parsing RSS data.
    """

    athena_results_bucket: s3.Bucket
    raw_bucket: s3.Bucket
    staging_bucket: s3.Bucket
    scripts_bucket: s3.Bucket

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

        self.raw_bucket = s3.Bucket(
            scope=self,
            id="RawBucket",
            bucket_name=f"{construct_id}-raw",
            removal_policy=RemovalPolicy.DESTROY,  # depending on use case, don't delete everything when stack is destroyed
            event_bridge_enabled=True,  # This is to trigger the glue job to parse xml file and convert that data to csv format
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[rule],
            # Encryption enabled by default by AWS on the server side.
            # I am doing explict work for learning purposes.
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        self.staging_bucket = s3.Bucket(
            scope=self,
            id="StagingBucket",
            bucket_name=f"{construct_id}-staging",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[rule],
        )

        self.scripts_bucket = s3.Bucket(
            scope=self,
            id="ScriptsBucket",
            bucket_name=f"{construct_id}-scripts",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
        )

        scripts_path = os.path.join("lib", "scripts")

        s3_deploy.BucketDeployment(
            scope=self,
            id=f"{construct_id}-deploy-scripts",
            sources=[s3_deploy.Source.asset(path=scripts_path)],
            destination_bucket=self.scripts_bucket,
            prune=True,
        )

        self.athena_results_bucket = s3.Bucket(
            scope=self,
            id="AthenaResultsBucket",
            bucket_name=f"{construct_id}-athena-results",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
