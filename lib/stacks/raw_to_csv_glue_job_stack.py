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

        athena_table = glue.CfnTable(
            scope=self,
            id="CampusEventsEventsTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=glue_db_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="campus_events_prod_s3_staging",
                description="Campus events staging table with pipe delimiter",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "csv",
                    "typeOfData": "file",
                    "skip.header.line.count": "1",
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(
                            name="record_source", type="string"
                        ),
                        glue.CfnTable.ColumnProperty(name="load_date", type="string"),
                        glue.CfnTable.ColumnProperty(name="event_id", type="bigint"),
                        glue.CfnTable.ColumnProperty(name="title", type="string"),
                        glue.CfnTable.ColumnProperty(name="host", type="string"),
                        glue.CfnTable.ColumnProperty(name="start_date", type="string"),
                        glue.CfnTable.ColumnProperty(name="end_date", type="string"),
                        glue.CfnTable.ColumnProperty(name="start_time", type="string"),
                        glue.CfnTable.ColumnProperty(name="end_time", type="string"),
                        glue.CfnTable.ColumnProperty(
                            name="event_description", type="string"
                        ),
                        glue.CfnTable.ColumnProperty(name="location", type="string"),
                        glue.CfnTable.ColumnProperty(name="link", type="string"),
                        glue.CfnTable.ColumnProperty(name="categories", type="string"),
                    ],
                    location=f"s3://{props.staging_bucket.bucket_name}/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.serde2.OpenCSVSerde",
                        parameters={
                            "separatorChar": "|",
                            "quoteChar": '"',
                            "escapeChar": "\\",
                        },
                    ),
                ),
            ),
        )
        athena_table.add_dependency(glue_db)

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
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{props.athena_results_bucket.bucket_name}/athena-results/"
                ),
            ),
        )
