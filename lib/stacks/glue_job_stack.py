from dataclasses import dataclass
from aws_cdk import Stack, aws_glue as glue, aws_iam as iam
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

        glue_rule = iam.Role(
            scope=self,
            id=f"{construct_id}-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )

        props.scripts_bucket.grant_read(glue_rule)

        glue.CfnJob(
            scope=self,
            id=f"{construct_id}-job",
            role=glue_rule.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{props.scripts_bucket.bucket_name}/scripts/transform_xml_to_csv.py",
            ),
        )
