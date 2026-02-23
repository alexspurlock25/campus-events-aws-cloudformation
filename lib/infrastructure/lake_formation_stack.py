"""
AWS Lake Formation stack for centralized data lake governance.
Implements fine-grained access control and data catalog management.
"""

from typing import Any, Dict, List

from aws_cdk import (
    Aws,
    CfnOutput,
    Stack,
)
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lakeformation as lf
from aws_cdk import aws_s3 as s3
from constructs import Construct


class LakeFormationStack(Stack):
    """
    Lake Formation governance stack implementing:
    - Data lake administrators
    - Database and table permissions
    - Location-based permissions for S3 buckets
    - Cross-service integration roles
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bronze_bucket: s3.Bucket,
        silver_bucket: s3.Bucket,
        **kwargs,
    ) -> None:
        super().__init__(
            scope, construct_id, stack_name="CampusEventsLakeFormation", **kwargs
        )

        # # Create data lake administrators
        # self.data_lake_admins = self._create_data_lake_administrators(construct_id)

        # Register S3 locations with Lake Formation
        self._register_s3_locations(construct_id, bronze_bucket, silver_bucket)

    #     self._create_database_permissions(construct_id)

    def _create_data_lake_administrators(self, construct_id: str) -> List[iam.Role]:
        """
        Create data lake administrator roles
        """

        data_admin_role = iam.Role(
            self,
            "DataLakeAdminRole",
            role_name=f"{construct_id}-admin-role",
            assumed_by=iam.AccountRootPrincipal(),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("LakeFormationDataAdmin")
            ],
        )

        etl_role = iam.Role(
            self,
            "ETLServiceRole",
            role_name=f"{construct_id}-etl-service-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        lf.CfnDataLakeSettings(
            self,
            "DataLakeSettings",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=data_admin_role.role_arn
                ),
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=etl_role.role_arn
                ),
            ],
        )

        return [data_admin_role, etl_role]

    def _register_s3_locations(
        self, construct_id: str, bronze_bucket: s3.Bucket, silver_bucket: s3.Bucket
    ) -> None:
        """Register S3 bucket locations with Lake Formation"""

        # Register bronze bucket location
        lf.CfnResource(
            scope=self,
            id="BronzeBucketResource",
            resource_arn=bronze_bucket.bucket_arn,
            use_service_linked_role=True,
        )

        # Register silver bucket location
        lf.CfnResource(
            scope=self,
            id="SilverBucketResource",
            resource_arn=silver_bucket.bucket_arn,
            use_service_linked_role=True,
        )

    # def _create_database_permissions(self, construct_id: str) -> None:
    #     """Create database-level permissions for different user groups"""

    #     # Database permissions for ETL role
    #     lf.CfnPermissions(
    #         self,
    #         "ETLDatabasePermissions",
    #         data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
    #             data_lake_principal_identifier=self.data_lake_admins[
    #                 1
    #             ].role_arn  # ETL role
    #         ),
    #         resource=lf.CfnPermissions.ResourceProperty(
    #             database_resource=lf.CfnPermissions.DatabaseResourceProperty(
    #                 catalog_id=Aws.ACCOUNT_ID, name=f"{construct_id}-silver-db"
    #             )
    #         ),
    #         permissions=["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"],
    #     )
