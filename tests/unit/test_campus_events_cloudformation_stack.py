import aws_cdk as core
import aws_cdk.assertions as assertions

from campus_events_cloudformation.campus_events_cloudformation_stack import (
    CampusEventsCloudformationStack,
)


def test_lambda_created():
    app = core.App()
    stack = CampusEventsCloudformationStack(app, "campus-events-cloudformation")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {"Handler": "hello_world.hello_world_handler", "Runtime": "python3.12"},
    )
