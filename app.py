#!/usr/bin/env python3
# For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
import sys

from aws_cdk import App, Tags

from lib.config import load_environment_config, load_projecttoml_config
from lib.infrastructure.stacks import (
    DataLakeStack,
    GlueResourcesStack,
    AnalyticsResourcesStack,
)
from lib.pipeline.stacks import (
    GetRssLambdaStack,
    BronzeToSilverGlueJobStack,
    BronzeToSilverGlueJobStackParamProps,
    DynamoDBStack,
    DynamoDBStackParamProps,
)

app = App()

env_name = (
    app.node.try_get_context("environment")
    or "prod"  # should be dev if env is missing in the real world
)
env_config = load_environment_config(environment=env_name)
project_config = load_projecttoml_config()

if env_config is None:
    print(f"Configuration for environment '{env_name}' not found.", file=sys.stderr)
    sys.exit(1)

if project_config is None:
    print("Something went wrong while reading your pyproject.py file.", file=sys.stderr)
    sys.exit(1)

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

glue_job_stack = BronzeToSilverGlueJobStack(
    scope=app,
    construct_id="-".join([root_construct_id, "bronze-to-silver-glue"]),
    props=BronzeToSilverGlueJobStackParamProps(
        bronze_bucket=dl_stack.bronze_bucket,
        silver_bucket=dl_stack.silver_bucket,
        athena_results_bucket=analytics_stack.athena_results_bucket,
        scripts_bucket=glue_scripts_stack.scripts_bucket,
    ),
)
glue_job_stack.add_dependency(dl_stack)
glue_job_stack.add_dependency(analytics_stack)
glue_job_stack.add_dependency(glue_scripts_stack)

# dynamo_db_stack = DynamoDBStack(
#     scope=app,
#     construct_id="-".join([root_construct_id, "dynamodb"]),
#     props=DynamoDBStackParamProps(staging_bucket=s3_csv_stack.staging_bucket),
# )

Tags.of(app).add("Project", "RssPipeline")
Tags.of(app).add("Environment", env_config.environment)
Tags.of(app).add("ManagedBy", "CDK")

app.synth()
