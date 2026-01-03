import ast
import csv
import json
import os
from datetime import datetime
from io import StringIO
from typing import Any

# boto3==1.42.5
import boto3

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def get_dynamo_table(table_name: str):
    return dynamodb.Table(table_name)


def parse_categories(categories_str):
    """
    Parse categories from string representation to actual list.
    Input: "['Sports', 'Campus Life']"
    Output: ['Sports', 'Campus Life']
    """
    if not categories_str or categories_str.strip() == "":
        return []

    try:
        # Use ast.literal_eval to safely parse string to list
        categories_list = ast.literal_eval(categories_str)

        # Filter out empty strings
        return [cat.strip() for cat in categories_list if cat.strip()]
    except (ValueError, SyntaxError):
        # If parsing fails, treat as single category or empty
        print(f"Warning: Could not parse categories: {categories_str}")
        return [categories_str] if categories_str else []


def handler(event, context):
    print(f"Event received: {json.dumps(event)}")

    bucket_name: str
    key: str
    table: Any

    try:
        bucket_name = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]
        table = get_dynamo_table(os.environ["TABLE_NAME"])
        print(f"Processing file: s3://{bucket_name}/{key}")

    except (KeyError, IndexError) as e:
        print(f"Error parsing event: {e}")
        return {"statusCode": 400, "body": json.dumps("Invalid event structure")}

    if not key.endswith(".csv"):
        print(f"Skipping none-csv file: {key}")
        return {"statusCode": 200, "body": json.dumps("Skipped non-CSV file")}

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response["Body"].read().decode("utf-8")
        print(f"Successfully read file, size: {len(csv_content)} bytes")

        csv_reader = csv.DictReader(StringIO(csv_content), delimiter="|", quotechar='"')

        inserted_count = 0
        error_count = 0

        for row in csv_reader:
            try:
                item = {
                    "record_source": row["record_source"],
                    "load_date": row["load_date"],
                    "event_id": row["event_id"],
                    "title": row.get("title", ""),
                    "host": row.get("host", ""),
                    "start_date": row.get("start_date", ""),
                    "end_date": row.get("end_date", ""),
                    "start_time": row.get("start_time", ""),
                    "end_time": row.get("end_time", ""),
                    "event_description": row.get("event_description", ""),
                    "location": row.get("location", ""),
                    "link": row.get("link", ""),
                    "categories": parse_categories(row.get("categories", [])),
                }

                # Put item in DynamoDB (upsert - overwrites if eventId exists)
                table.put_item(Item=item)
                inserted_count += 1

                # Log progress every 100 items
                if inserted_count % 100 == 0:
                    print(f"Processed {inserted_count} items...")

            except Exception as row_error:
                error_count += 1
                print(f"Error processing row: {row_error}")
                print(f"Problematic row: {row}")

        print("Processing complete!")
        print(f"Successfully inserted/updated: {inserted_count} items")
        print(f"Errors: {error_count} items")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Successfully loaded events to DynamoDB",
                    "inserted": inserted_count,
                    "errors": error_count,
                    "source_file": key,
                }
            ),
        }

    except Exception as e:
        print(f"Error processing file: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Error: {str(e)}")}
