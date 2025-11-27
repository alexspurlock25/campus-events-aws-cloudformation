"""
Storing CSV files
"""

from aws_cdk import RemovalPolicy, Stack, Environment
from aws_cdk import aws_s3 as s3
from constructs import Construct

from lib.config import PipelineConfig


class S3CSVStack(Stack):
    """
    Stack for CSV storage after parsing RSS data.
    """

    csv_bucket: s3.Bucket

    def __init__(
        self, scope: Construct, id: str, env: Environment, config: PipelineConfig, **kwargs
    ) -> None:
        super().__init__(scope, id, env=env, **kwargs)

        self.csv_bucket = s3.Bucket(
            self,
            "CsvBucket",
            bucket_name=config.csv_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
        )
