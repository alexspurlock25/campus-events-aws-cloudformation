"""
This module contains the Lambda function that retrieves event names from the DynamoDB table. It is used by the API Gateway to provide a list of event names to the frontend application.
"""

import os
import json
import boto3


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["TABLE_NAME"])
    response = table.scan()
    event_names = [item["title"] for item in response["Items"]]

    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": None,
        "body": json.dumps(event_names),
    }
