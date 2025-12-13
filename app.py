#!/usr/bin/env python3
# For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
import os
import sys

from aws_cdk import App, Environment, Stack, Tags
from aws_cdk import aws_s3 as s3

from lib.config import load_environment_config, load_projecttoml_config
from lib.stacks import RssToCsvLambdaStack, S3CSVStack

app = App()

env_name = (
    app.node.try_get_context("environment") or "prod"
)  # should be dev in the real world
env_config = load_environment_config(environment=env_name)
project_config = load_projecttoml_config()

if env_config is None:
    print(f"Configuration for environment '{env_name}' not found.", file=sys.stderr)
    sys.exit(1)

if project_config is None:
    print("Something went wrong while reading your pyproject.py file.", file=sys.stderr)
    sys.exit(1)

root_construct_id = "-".join([project_config.project_name, env_config.environment])

s3_csv_stack = S3CSVStack(
    scope=app,
    construct_id="-".join([root_construct_id, "s3"]),
    config=env_config,
)

lambda_stack = RssToCsvLambdaStack(
    scope=app,
    construct_id="-".join([root_construct_id, "lambda"]),
    config=env_config,
    csv_bucket=s3_csv_stack.raw_bucket,
)
lambda_stack.add_dependency(s3_csv_stack)

Tags.of(app).add("Project", "RssPipeline")
Tags.of(app).add("Environment", env_config.environment)
Tags.of(app).add("ManagedBy", "CDK")

app.synth()
