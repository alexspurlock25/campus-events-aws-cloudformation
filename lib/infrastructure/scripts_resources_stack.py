"""
Infrastructure stack for Glue ETL scripts and transformations.
Deploys and manages S3 bucket for Glue job scripts.
"""

import os

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deploy
from constructs import Construct


class ScriptsResourcesStack(Stack):
    """
    Infrastructure stack for Glue ETL scripts and transformations.
    Deploys and manages S3 bucket for Glue job scripts.
    """

    scripts_bucket: s3.Bucket

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope, construct_id, stack_name="UCEventsResourcesStack", **kwargs
        )

        self.scripts_bucket = s3.Bucket(
            scope=self,
            id="ScriptsBucket",
            bucket_name=f"{construct_id}-bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        scripts_path = os.path.join("lib", "pipeline", "scripts")

        s3_deploy.BucketDeployment(
            scope=self,
            id="ScriptsBucketDeployment",
            sources=[s3_deploy.Source.asset(path=scripts_path)],
            destination_bucket=self.scripts_bucket,
            prune=True,
        )
