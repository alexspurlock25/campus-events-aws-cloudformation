"""
Function list and return all files ending with .xml in the bronze bucket but are not in the /processed/ folder and return the list to the state machine map.
"""

import boto3


def handler(event, context):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=event["BRONZE_BUCKET"], Prefix="new/")
    files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".xml")]
    return files
