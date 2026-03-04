"""
Data lake infrastructure with Lake Formation governance.
Manages bronze and silver S3 buckets following medallion architecture.
"""

from aws_cdk import Aws, Duration, RemovalPolicy, Stack, aws_glue, aws_s3
from aws_cdk import aws_athena as athena
from constructs import Construct


class DataLakeStack(Stack):
    """
    Data lake infrastructure with Lake Formation governance.
    Manages bronze and silver S3 buckets following medallion architecture.
    """

    bronze_bucket: aws_s3.Bucket
    silver_bucket: aws_s3.Bucket
    athena_results_bucket: aws_s3.Bucket
    glue_db_name: str

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope, construct_id, stack_name="UCEventsDataLakeStack", **kwargs
        )

        # ------------------------------------------------------------------
        # S3 BUCKETS (Bronze and Silver)
        # ------------------------------------------------------------------

        life_cycle_rule = aws_s3.LifecycleRule(
            id=f"{construct_id}-lifecycle-rule",
            transitions=[
                # First move to an IA S3 (stays here for 30 days)
                aws_s3.Transition(
                    storage_class=aws_s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=Duration.days(30),
                ),
                # Then Move to Glacier (stays here for 30 days)
                aws_s3.Transition(
                    storage_class=aws_s3.StorageClass.GLACIER,
                    transition_after=Duration.days(60),
                ),
            ],
            # Delete after 90 days from creation
            expiration=Duration.days(90),
        )

        self.bronze_bucket = aws_s3.Bucket(
            scope=self,
            id="BronzeBucket",
            bucket_name=f"{construct_id}-bronze-bucket",
            removal_policy=RemovalPolicy.DESTROY,  # depending on use case, don't delete everything when stack is destroyed
            lifecycle_rules=[life_cycle_rule],
        )

        self.silver_bucket = aws_s3.Bucket(
            scope=self,
            id="SilverBucket",
            bucket_name=f"{construct_id}-silver-bucket",
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[life_cycle_rule],
        )

        self.athena_results_bucket = aws_s3.Bucket(
            scope=self,
            id="AthenaResults",
            bucket_name=f"{construct_id}-athena-results-bucket",
            removal_policy=RemovalPolicy.DESTROY,
        )

        athena.CfnWorkGroup(
            scope=self,
            id="AthenaWorkgroup",
            name=f"{construct_id}-athena-workgroup",
            state="ENABLED",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.athena_results_bucket.bucket_name}/athena-results/"
                ),
            ),
        )

        self.glue_db_name = f"{construct_id}-glue-database"

        aws_glue.CfnDatabase(
            scope=self,
            id="UCEventsDatabase",
            catalog_id=Aws.ACCOUNT_ID,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name=self.glue_db_name,
                description="Database for UC Events",
                location_uri=f"s3://{self.silver_bucket.bucket_name}/",
            ),
        )
