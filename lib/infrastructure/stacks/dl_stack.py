"""
Data lake infrastructure with Lake Formation governance.
Manages bronze and silver S3 buckets following medallion architecture.
"""

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_s3, aws_iam, aws_glue
from aws_cdk import aws_lakeformation, Aws
from constructs import Construct


class DataLakeStack(Stack):
    """
    Data lake infrastructure with Lake Formation governance.
    Manages bronze and silver S3 buckets following medallion architecture.
    """

    bronze_bucket: aws_s3.Bucket
    silver_bucket: aws_s3.Bucket

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(
            scope, construct_id, stack_name="CampusEventsDataLake", **kwargs
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
            bucket_name=f"{construct_id}-bronze",
            removal_policy=RemovalPolicy.DESTROY,  # depending on use case, don't delete everything when stack is destroyed
            event_bridge_enabled=True,  # This is to trigger the glue job to parse xml file and convert that data to csv format
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[life_cycle_rule],
            # Encryption enabled by default by AWS on the server side.
            # I am doing explict work for learning purposes.
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
        )

        self.silver_bucket = aws_s3.Bucket(
            scope=self,
            id="SilverBucket",
            bucket_name=f"{construct_id}-silver",
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True,
            auto_delete_objects=True,
            versioned=True,
            lifecycle_rules=[life_cycle_rule],
        )

        # ------------------------------------------------------------------
        # ROLES
        # ------------------------------------------------------------------

        lake_formation_admin_role = aws_iam.Role(
            scope=self,
            id="LakeFormationAdminRole",
            role_name=f"{construct_id}-lake-formation-admin",
            assumed_by=aws_iam.AccountPrincipal(Aws.ACCOUNT_ID),
            description="Lake Formation admin role (infra only)",
        )
        lake_formation_admin_role.add_to_policy(
            # give access to all actions within lake formation domain
            aws_iam.PolicyStatement(
                actions=["lakeformation:*"],
                resources=["*"],
            )
        )

        silver_etl_role = aws_iam.Role(
            scope=self,
            id="SilverETLRole",
            role_name=f"{construct_id}-silver-etl",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            description="Glue ETL role for silver upserts.",
        )
        silver_etl_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"{self.silver_bucket.bucket_arn}",
                    f"{self.silver_bucket.bucket_arn}/*",
                ],
            )
        )

        silver_etl_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        # ------------------------------------------------------------------
        # Glue DB (Silver bucket)
        # ------------------------------------------------------------------

        silver_db_name = f"{construct_id}-silver-db"
        aws_glue.CfnDatabase(
            scope=self,
            id="SilverDatabase",
            database_name=silver_db_name,
            catalog_id=Aws.ACCOUNT_ID,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name=silver_db_name
            ),
        )
        silver_etl_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:UpdatePartition",
                ],
                resources=[
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{silver_db_name}",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{silver_db_name}/*",
                ],
            )
        )

        # ------------------------------------------------------------------
        # LAKE FORMATION GOVERNANCE
        # ------------------------------------------------------------------
        aws_lakeformation.CfnDataLakeSettings(
            self,
            "LakeFormationSettings",
            admins=[
                aws_lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=lake_formation_admin_role.role_arn
                )
            ],
        )

        aws_lakeformation.CfnResource(
            self,
            "SilverLakeFormationResource",
            resource_arn=self.silver_bucket.bucket_arn,
            role_arn=lake_formation_admin_role.role_arn,
            use_service_linked_role=False,
        )

        aws_lakeformation.CfnPermissions(
            self,
            "SilverDBPermissions",
            data_lake_principal=aws_lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=silver_etl_role.role_arn
            ),
            resource=aws_lakeformation.CfnPermissions.ResourceProperty(
                database_resource=aws_lakeformation.CfnPermissions.DatabaseResourceProperty(
                    name=silver_db_name
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DESCRIBE"],
        )
