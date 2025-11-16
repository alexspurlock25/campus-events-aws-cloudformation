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
        {"Handler": "fetch_new_events.lambda_handler", "Runtime": "python3.9"},
    )


def test_lambda_is_private():
    app = core.App()
    stack = CampusEventsCloudformationStack(app, "campus-events-cloudformation")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::Lambda::Url", 0)
    template.resource_count_is("AWS::Lambda::FunctionUrl", 0)

    # Ensure no public Lambda permissions
    for resource in template.find_resources("AWS::Lambda::Permission").values():
        assert resource["Properties"]["Principal"] != "*"
