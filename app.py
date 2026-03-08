#!/usr/bin/env python3
# For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
from aws_cdk import App

from lib.config import load_environment_config, load_projecttoml_config
from lib.infrastructure import (
    DataLakeStack,
    ScriptsResourcesStack,
    ApiStack,
    ApiStackProps,
)
from lib.pipeline.stacks import (
    BronzeToSilverWorkflowStack,
    BronzeToSilverWorkflowStackProps,
    RssPipelineOrchestratorStack,
    RssPipelineOrchestratorStackProps,
    SilverToDynamoEventsWorkflowStack,
    SilverToDynamoEventsWorkflowStackProps,
)

app = App()

env_config = load_environment_config()
app_config = load_projecttoml_config()

dl_stack = DataLakeStack(scope=app, construct_id=f"{app_config.project_name}-dl")

glue_scripts_stack = ScriptsResourcesStack(
    scope=app, construct_id=f"{app_config.project_name}-script-resources"
)

bronze_to_silver_wf = BronzeToSilverWorkflowStack(
    scope=app,
    construct_id=f"{app_config.project_name}-brz-to-slv-wf",
    props=BronzeToSilverWorkflowStackProps(
        bronze_bucket=dl_stack.bronze_bucket,
        silver_bucket=dl_stack.silver_bucket,
        silver_db_name=dl_stack.glue_db_name,
        athena_results_bucket=dl_stack.athena_results_bucket,
        scripts_bucket=glue_scripts_stack.scripts_bucket,
        notification_email=env_config.email,
    ),
)
bronze_to_silver_wf.add_dependency(dl_stack)
bronze_to_silver_wf.add_dependency(glue_scripts_stack)

silver_to_dynamo_wf = SilverToDynamoEventsWorkflowStack(
    scope=app,
    construct_id=f"{app_config.project_name}-slv-to-dynamo-wf",
    props=SilverToDynamoEventsWorkflowStackProps(
        silver_bucket=dl_stack.silver_bucket,
        scripts_bucket=glue_scripts_stack.scripts_bucket,
        notification_email=env_config.email,
    ),
)
silver_to_dynamo_wf.add_dependency(dl_stack)
silver_to_dynamo_wf.add_dependency(glue_scripts_stack)

orchestrator_stack = RssPipelineOrchestratorStack(
    scope=app,
    construct_id=f"{app_config.project_name}-orchestrator",
    props=RssPipelineOrchestratorStackProps(
        config=env_config,
        bronze_bucket=dl_stack.bronze_bucket,
        bronze_to_silver_state_machine=bronze_to_silver_wf.state_machine,
        silver_to_dynamo_state_machine=silver_to_dynamo_wf.state_machine,
    ),
)
orchestrator_stack.add_dependency(dl_stack)
orchestrator_stack.add_dependency(bronze_to_silver_wf)
orchestrator_stack.add_dependency(silver_to_dynamo_wf)

api_stack = ApiStack(
    scope=app,
    id=f"{app_config.project_name}-api",
    props=ApiStackProps(
        dynamodb_table=silver_to_dynamo_wf.events_table,
    ),
)
api_stack.add_dependency(orchestrator_stack)

app.synth()
