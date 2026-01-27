"""
Data lake infrastructure with Lake Formation governance.
Manages bronze and silver S3 buckets following medallion architecture.
"""

import os

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3, aws_iam as iam, CfnOutput
from aws_cdk import aws_lakeformation as lf
from constructs import Construct


class DataLakeStack(Stack):
    """
    Data lake infrastructure with Lake Formation governance.
    Manages bronze and silver S3 buckets following medallion architecture.
    """

    bronze_bucket: s3.Bucket
    silver_bucket: s3.Bucket

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope, construct_id, stack_name="CampusEventsDataLake", **kwargs
        )

        life_cycle_rule = s3.LifecycleRule(
            id=f"{construct_id}-lifecycle-rule",
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

        self.bronze_bucket = s3.Bucket(
            scope=self,
            id="BronzeBucket",
            bucket_name=f"{construct_id}-bronze",
            removal_policy=RemovalPolicy.DESTROY,  # depending on use case, don't delete everything when stack is destroyed
            event_bridge_enabled=True,  # This is to trigger the glue job to parse xml file and convert that data to csv format
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[life_cycle_rule],
            # Encryption enabled by default by AWS on the server side.
            # I am doing explict work for learning purposes.
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        self.silver_bucket = s3.Bucket(
            scope=self,
            id="SilverBucket",
            bucket_name=f"{construct_id}-silver",
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True,
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[life_cycle_rule],
        )

        # let lake formation have access to s3 buckets
        dl_role = iam.Role(
            scope=self,
            id="CampusEventsDataLakeRole",
            role_name=f"{construct_id}-dl-role",
            assumed_by=iam.ServicePrincipal(
                "lakeformation.amazonaws.com"
            ),  # Defines who is allowed to assume this role
            description="Service role for Lake Formation data lake operations",
        )

        self.bronze_bucket.grant_read_write(dl_role)
        self.silver_bucket.grant_read_write(dl_role)

        self.bronze_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCampusEventsDataLakeBronzeBucketAccess",
                effect=iam.Effect.ALLOW,
                principals=[iam.ArnPrincipal(dl_role.role_arn)],
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    self.bronze_bucket.bucket_arn,
                    f"{self.bronze_bucket.bucket_arn}/*",
                ],
            )
        )

        self.silver_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowCampusEventsDataLakeSilverBucketAccess",
                effect=iam.Effect.ALLOW,
                principals=[iam.ArnPrincipal(dl_role.role_arn)],
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    self.silver_bucket.bucket_arn,
                    f"{self.silver_bucket.bucket_arn}/*",
                ],
            )
        )

        lf.CfnResource(
            scope=self,
            id="CampusEventsBronzeBucketDataLakeResource",
            resource_arn=self.bronze_bucket.bucket_arn,
            role_arn=dl_role.role_arn,
            use_service_linked_role=False,
        )

        lf.CfnResource(
            scope=self,
            id="CampusEventsSilverBucketDataLakeResource",
            resource_arn=self.silver_bucket.bucket_arn,
            role_arn=dl_role.role_arn,
            use_service_linked_role=False,
        )

        lf.CfnDataLakeSettings(
            scope=self,
            id="CampusEventsDataLakeSettings",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=dl_role.role_arn
                )
            ],
        )

        dl_role.apply_removal_policy(RemovalPolicy.DESTROY)

        CfnOutput(
            scope=self,
            id="CampusEventsDataLakeRoleArn",
            value=dl_role.role_arn,
            export_name=f"{construct_id}-dl-role-arn",
            description="Lake Formation service role ARN for cross-stack access",
        )
