"""
Lambda hanlder to convert rss data to csv
"""

import json
import os
from datetime import datetime, timezone
from urllib import request

import boto3

s3_client = boto3.client("s3")


def handler(event, context):
    """
    Lambda handler to fetch rss data and put
    it into data lake bronze bucket
    """

    rss_url = os.environ["RSS_FEED_URL"]
    rss_feed_name = os.environ["RSS_FEED_NAME"]
    bucket_name = os.environ["BRONZE_BUCKET_NAME"]

    try:
        rss_response = request.urlopen(rss_url, timeout=30)
        content = rss_response.read()

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{rss_feed_name}/events_{timestamp}.xml"

        s3_client.put_object(
            Bucket=bucket_name, Key=s3_key, Body=content, ContentType="application/xml"
        )

        print(f"Successfully uploaded RSS data to s3://{bucket_name}/{s3_key}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "RSS feed processed successfully",
                    "s3_location": f"s3://{bucket_name}/{s3_key}",
                    "content_size": len(content),
                }
            ),
        }
    except Exception as e:
        print(f"Error processing RSS feed: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
