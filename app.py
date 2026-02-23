#!/usr/bin/env python3
# For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
import sys

from aws_cdk import App

from lib.config import load_environment_config, load_projecttoml_config
from lib.infrastructure import DataLakeStack, LakeFormationStack, ScriptsResourcesStack
from lib.pipeline.stacks import (
    BronzeToSilverWorkflowStack,
    BronzeToSilverWorkflowStackProps,
    GetRssLambdaStack,
    GetRssLambdaStackProps,
    SilverToDynamoEventsWorkflowStack,
    SilverToDynamoEventsWorkflowStackProps,
)

app = App()

env_config = load_environment_config()
app_config = load_projecttoml_config()

dl_stack = DataLakeStack(scope=app, construct_id=f"{app_config.project_name}-dl")

lf_stack = LakeFormationStack(
    scope=app,
    construct_id=f"{app_config.project_name}-lf",
    bronze_bucket=dl_stack.bronze_bucket,
    silver_bucket=dl_stack.silver_bucket,
)
lf_stack.add_dependency(dl_stack)

glue_scripts_stack = ScriptsResourcesStack(
    scope=app, construct_id=f"{app_config.project_name}-script-resources"
)
analytics_stack.add_dependency(dl_stack)

lambda_stack = GetRssLambdaStack(
    scope=app,
    construct_id=f"{app_config.project_name}-get-rss",
    props=GetRssLambdaStackProps(
        config=env_config, bronze_bucket=dl_stack.bronze_bucket
    ),
)
lambda_stack.add_dependency(dl_stack)

bronze_to_silver_wf = BronzeToSilverWorkflowStack(
    scope=app,
    construct_id=f"{app_config.project_name}-brz-to-slv-wf",
    props=BronzeToSilverWorkflowStackProps(
        bronze_bucket=dl_stack.bronze_bucket,
        silver_bucket=dl_stack.silver_bucket,
        athena_results_bucket=dl_stack.athena_results_bucket,
        scripts_bucket=glue_scripts_stack.scripts_bucket,
        notification_email=env_config.email,
    ),
)
bronze_to_silver_wf.add_dependency(dl_stack)
bronze_to_silver_wf.add_dependency(glue_scripts_stack)

dynamo_db_stack = SilverToDynamoEventsWorkflowStack(
    scope=app,
    construct_id="-".join([root_construct_id, "silver-to-dynamo-events-wf"]),
    props=SilverToDynamoEventsWorkflowStackProps(
        bronze_bucket=dl_stack.bronze_bucket,
        silver_bucket=dl_stack.silver_bucket,
        scripts_bucket=glue_scripts_stack.scripts_bucket,
    ),
)
dynamo_db_stack.add_dependency(dl_stack)

app.synth()
