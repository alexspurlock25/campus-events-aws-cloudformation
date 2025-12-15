from dataclasses import dataclass
from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
)
from aws_cdk.aws_s3 import IBucket


@dataclass
class RawToCsvGlueJobStackParamProps:
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
            id=f"{construct_id}-role",
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

        job = glue.CfnJob(
            scope=self,
            id=f"{construct_id}-job",
            role=glue_role.role_arn,
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{props.scripts_bucket.bucket_name}/scripts/transform_xml_to_csv.py",
            ),
            default_arguments={
                "--enable-spark-ui": True,
                "--enable-metrics": True,
                "--enable-continuous-cloudwatch-log": True,
                "--additional-python-modules": "feedparser, beautifulsoup4",
                "--SOURCE_BUCKET_NAME": props.raw_bucket.bucket_name,
                "--TARGET_BUCKET_NAME": props.staging_bucket.bucket_name,
            },
        )

        rule = events.Rule(
            scope=self,
            id=f"{construct_id}-on-raw-put-rule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [props.raw_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".xml"}]},
                },
            ),
        )

        rule.add_target(
            targets.AwsApi(
                service="Glue", action="startJobRun", parameters={"JobName": job.ref}
            )
        )
