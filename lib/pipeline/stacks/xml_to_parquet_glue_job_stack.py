from dataclasses import dataclass

from aws_cdk import Aws, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lakeformation as lf
from aws_cdk.aws_s3 import IBucket


@dataclass
class BronzeToSilverGlueJobStackParamProps:
    bronze_bucket: IBucket
    silver_bucket: IBucket
    athena_results_bucket: IBucket
    scripts_bucket: IBucket


class BronzeToSilverGlueJobStack(Stack):
    def __init__(
        self, scope, construct_id, props: BronzeToSilverGlueJobStackParamProps, **kwargs
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            stack_name="CampusEventsBronzeToSilverJobResources",
            **kwargs,
        )

        glue_role = iam.Role(
            scope=self,
            id="BronzeToSilverJobServiceRole",
            role_name=f"{construct_id}-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        props.scripts_bucket.grant_read(glue_role)
        props.bronze_bucket.grant_read_write(glue_role)
        props.silver_bucket.grant_read_write(glue_role)

        glue_job_name = f"{construct_id}-job"
        job = glue.CfnJob(
            scope=self,
            id="BronzeToSilverJob",
            name=glue_job_name,
            role=glue_role.role_arn,
            glue_version="5.1",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=5,
            max_retries=0,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{props.scripts_bucket.bucket_name}/xml_to_parquet.py",
            ),
            default_arguments={
                "--job-language": "python",
                "--TempDir": f"s3://{props.bronze_bucket.bucket_name}/xml_to_parquet_logs/",
                "--continuous-log-logGroup": f"/aws-glue/jobs/{glue_job_name}",
                "--enable-spark-ui": "true",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--additional-python-modules": "feedparser,beautifulsoup4,pyarrow",
                "--SOURCE_BUCKET_NAME": props.bronze_bucket.bucket_name,
                "--TARGET_BUCKET_NAME": props.silver_bucket.bucket_name,
            },
        )

        rule = events.Rule(
            scope=self,
            id="OnBronzeNewObjectEventRule",
            rule_name=f"{construct_id}-on-bronze-put-rule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [props.bronze_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".xml"}]},
                },
            ),
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
