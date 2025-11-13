#!/usr/bin/env python3
# For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
import os

import aws_cdk as cdk

from campus_events_cloudformation.campus_events_cloudformation_stack import (
    CampusEventsCloudformationStack,
)

app = cdk.App()
CampusEventsCloudformationStack(
    app,
    "CampusEventsCloudformationStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
    ),
)

app.synth()
