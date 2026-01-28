"""
Data lake infrastructure with Lake Formation governance.
Manages bronze and silver S3 buckets following medallion architecture.
"""

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3, aws_iam as iam, CfnOutput, aws_glue
from aws_cdk import aws_lakeformation as lf, Aws
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
            id="DataLakeRole",
            role_name=f"{construct_id}-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal(
                    "lakeformation.amazonaws.com",
                ),
                iam.ServicePrincipal(
                    "glue.amazonaws.com",
                ),
            ),  # Defines who is allowed to assume this role
            description="Service role for Lake Formation data lake operations",
        )

        # Grant Glue crawler permissions to write logs
        dl_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[
                    f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws-glue/crawlers:*",
                ],
            )
        )

        # Grant S3 read permissions for the crawler
        dl_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                resources=[
                    f"{self.silver_bucket.bucket_arn}/*",
                ],
            )
        )

        self.silver_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="AllowDataLakeSilverBucketFullCRUDAccess",
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

        dl_role.apply_removal_policy(RemovalPolicy.DESTROY)

        silver_db_name = f"{construct_id}-silver-db"
        glue_db = aws_glue.CfnDatabase(
            scope=self,
            id="SilverGlueDatabase",
            database_name=silver_db_name,
            catalog_id=Aws.ACCOUNT_ID,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name=silver_db_name
            ),
        )

        lf_silver_resource = lf.CfnResource(
            scope=self,
            id="SilverBucketDataLakeResource",
            resource_arn=self.silver_bucket.bucket_arn,
            role_arn=dl_role.role_arn,
            use_service_linked_role=False,
        )
        lf_silver_resource.add_dependency(glue_db)

        dl_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:BatchGetPartition",
                    "glue:BatchCreatePartition",
                ],
                resources=[
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{silver_db_name}",
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{silver_db_name}/*",
                ],
            )
        )

        silver_bucket_crawler = aws_glue.CfnCrawler(
            scope=self,
            id="SilverCrawler",
            name=f"{construct_id}-silver-crawler",
            role=dl_role.role_arn,
            database_name=silver_db_name,
            targets=aws_glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    aws_glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.silver_bucket.bucket_name}/"
                    )
                ]
            ),
            schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
            recrawl_policy=aws_glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_EVERYTHING"
            ),
        )
        silver_bucket_crawler.add_dependency(glue_db)

        lf_db_permissions = lf.CfnPermissions(
            scope=self,
            id="SilverDataLakeDatabasePermissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=dl_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    name=silver_db_name
                )
            ),
            permissions=[
                "CREATE_TABLE",
                "ALTER",
                "DESCRIBE",
            ],
        )
        lf_db_permissions.add_dependency(glue_db)
