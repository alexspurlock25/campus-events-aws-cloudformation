from dataclasses import dataclass

from aws_cdk import Aws, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk.aws_s3 import IBucket


@dataclass
class RawToCsvGlueJobStackParamProps:
    athena_results_bucket: IBucket
    raw_bucket: IBucket
    staging_bucket: IBucket
    scripts_bucket: IBucket


class RawToCsvGlueJobStack(Stack):
    def __init__(
        self, scope, construct_id, props: RawToCsvGlueJobStackParamProps, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        glue_role = iam.Role(
            scope=self,
            id="CampusEventsGlueServiceRole",
            role_name=f"{construct_id}-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        props.scripts_bucket.grant_read(glue_role)
        props.raw_bucket.grant_read_write(glue_role)
        props.staging_bucket.grant_read_write(glue_role)

        glue_job_name = f"{construct_id}-job"
        job = glue.CfnJob(
            scope=self,
            id="XmlToCSVCampusEventsJob",
            name=glue_job_name,
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=5,
            max_retries=0,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{props.scripts_bucket.bucket_name}/transform/xml_to_csv.py",
            ),
            default_arguments={
                "--job-language": "python",
                "--TempDir": f"s3://{props.raw_bucket.bucket_name}/xml_to_csv_logs/",
                "--continuous-log-logGroup": f"/aws-glue/jobs/{glue_job_name}",
                "--enable-spark-ui": "true",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--additional-python-modules": "feedparser,beautifulsoup4",
                "--SOURCE_BUCKET_NAME": props.raw_bucket.bucket_name,
                "--TARGET_BUCKET_NAME": props.staging_bucket.bucket_name,
            },
        )

        glue_db_name = f"{construct_id}-database"
        glue_db = glue.CfnDatabase(
            scope=self,
            id="CampusEventsGlueDatabase",
            database_name=glue_db_name,
            catalog_id=Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name=glue_db_name),
        )

        crawler_role = iam.Role(
            scope=self,
            id="CampusEventsCrawlerServiceRole",
            role_name=f"{construct_id}-crawler-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        props.staging_bucket.grant_read(crawler_role)

        rule = events.Rule(
            scope=self,
            id="CampusEventsOnRawPutEventRule",
            rule_name=f"{construct_id}-on-raw-put-rule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [props.raw_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".xml"}]},
                },
            ),
        )

        eventbridge_role = iam.Role(
            scope=self,
            id="CampusEventsEventBridgeRole",
            role_name=f"{construct_id}-eventbridge-role",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        )

        eventbridge_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=[
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:job/{glue_job_name}"
                ],
            )
        )

        rule.add_target(
            targets.AwsApi(
                service="Glue",
                action="startJobRun",
                parameters={"JobName": job.ref},
                policy_statement=iam.PolicyStatement(
                    actions=["glue:StartJobRun"],
                    resources=[
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:job/{glue_job_name}"
                    ],
                ),
            )
        )

        athena.CfnWorkGroup(
            scope=self,
            id="CampusEventsWorkgroup",
            name=f"{construct_id}-workgroup",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{props.athena_results_bucket.bucket_name}/athena-results/"
                ),
            ),
        )

        glue_crawler = glue.CfnCrawler(
            scope=self,
            id="CampusEventsStagingBucketCrawler",
            name=f"{construct_id}-stage-crawler",
            role=crawler_role.role_arn,
            database_name=glue_db_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{props.staging_bucket.bucket_name}/"
                    )
                ]
            ),
            table_prefix="events_",
        )
        glue_crawler.add_dependency(glue_db)

        glue_crawler_success_rule = events.Rule(
            scope=self,
            id="CampusEventsConvertToCsvJobSucceededRule",
            rule_name=f"{construct_id}-job-succeeded-rule",
            event_pattern=events.EventPattern(
                source=["aws.glue"],
                detail_type=["Glue Job State Change"],
                detail={"jobName": [job.ref], "state": ["SUCCEEDED"]},
            ),
        )
        glue_crawler_success_rule.node.add_dependency(job)

        crawler_arn = (
            f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:crawler/{glue_crawler.name}"
        )
        eventbridge_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:StartCrawler"],
                resources=[crawler_arn],
            )
        )

        glue_crawler_success_rule.add_target(
            targets.AwsApi(
                service="Glue",
                action="startCrawler",
                parameters={"Name": glue_crawler.name},
                policy_statement=iam.PolicyStatement(
                    actions=["glue:StartCrawler"],
                    resources=[crawler_arn],
                ),
            )
        )
