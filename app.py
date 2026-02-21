#!/usr/bin/env python3
# For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
import sys

from aws_cdk import App

from lib.config import load_environment_config, load_projecttoml_config
from lib.infrastructure.stacks import (
    AnalyticsResourcesStack,
    DataLakeStack,
    GlueResourcesStack,
)
from lib.pipeline.stacks import (
    BronzeToSilverWorkflowStack,
    BronzeToSilverWorkflowStackProps,
    GetRssLambdaStack,
    SilverToDynamoEventsWorkflowStack,
    SilverToDynamoEventsWorkflowStackProps,
)

app = App()

env_config = load_environment_config()
app_config = load_projecttoml_config()

root_construct_id = "-".join([project_config.project_name, env_config.environment])


dl_stack = DataLakeStack(
    scope=app, construct_id="-".join([root_construct_id, "data-lake"])
)

glue_scripts_stack = GlueResourcesStack(
    scope=app, construct_id="-".join([root_construct_id, "glue"])
)
glue_scripts_stack.add_dependency(dl_stack)

analytics_stack = AnalyticsResourcesStack(
    scope=app, construct_id="-".join([root_construct_id, "analytics"])
)
analytics_stack.add_dependency(dl_stack)

lambda_stack = GetRssLambdaStack(
    scope=app,
    construct_id="-".join([root_construct_id, "lambda"]),
    config=env_config,
    bronze_bucket=dl_stack.bronze_bucket,
)
lambda_stack.add_dependency(dl_stack)

glue_job_stack = BronzeToSilverWorkflowStack(
    scope=app,
    construct_id="-".join([root_construct_id, "bronze-to-silver-wf"]),
    props=BronzeToSilverWorkflowStackProps(
        bronze_bucket=dl_stack.bronze_bucket,
        silver_bucket=dl_stack.silver_bucket,
        athena_results_bucket=analytics_stack.athena_results_bucket,
        scripts_bucket=glue_scripts_stack.scripts_bucket,
    ),
)
glue_job_stack.add_dependency(dl_stack)
glue_job_stack.add_dependency(analytics_stack)
glue_job_stack.add_dependency(glue_scripts_stack)

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
