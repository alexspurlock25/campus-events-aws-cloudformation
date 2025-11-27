#!/usr/bin/env python3
# For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
from aws_cdk import App, Environment, Tags
from lib.config import load_config
from lib.stacks import S3CSVStack, LambdaStack
import os

app = App()

env_name = app.node.try_get_context("environment") or "prod" # should be dev in the real world
config = load_config(environment=env_name)
aws_env = Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-2")
)

s3_csv_stack = S3CSVStack(
    scope=app,
    id=f"RssPipeline-Storage-{config.environment}",
    config=config,
    env=aws_env
)

lambda_stack = LambdaStack(
    scope=app,
    id=f"RssPipeline-Lambda-{config.environment}",
    config=config,
    csv_bucket=s3_csv_stack.csv_bucket,
    env=aws_env
)
lambda_stack.add_dependency(s3_csv_stack)

Tags.of(app).add("Project", "RssPipeline")
Tags.of(app).add("Environment", config.environment)
Tags.of(app).add("ManagedBy", "CDK")

app.synth()
