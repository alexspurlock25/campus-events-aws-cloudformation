import aws_cdk as core
import aws_cdk.assertions as assertions

from campus_events_cloudformation.campus_events_cloudformation_stack import CampusEventsCloudformationStack

# example tests. To run these tests, uncomment this file along with the example
# resource in campus_events_cloudformation/campus_events_cloudformation_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CampusEventsCloudformationStack(app, "campus-events-cloudformation")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
