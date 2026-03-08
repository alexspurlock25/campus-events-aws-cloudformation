import sys
from typing import List
from dataclasses import dataclass

import boto3
from awsglue.context import DataFrame, GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from uc_types import file_schema


@dataclass
class Args:
    """
    Data type for args coming from the glue job
    """

    job_name: str
    silver_bucket_name: str
    dynamo_table_name: str


_args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_BUCKET_NAME",
        "DYNAMO_TABLE",
    ],
)

args = Args(
    job_name=_args["JOB_NAME"],
    silver_bucket_name=_args["SILVER_BUCKET_NAME"],
    dynamo_table_name=_args["DYNAMO_TABLE"],
)

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
dynamo_table = dynamodb.Table(args.dynamo_table_name)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session


def current_dynamodb_data() -> DynamicFrame:
    """
    Reads the DynamoDB table and returns a DynamicFrame
    """

    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={
            "dynamodb.input.tableName": args.dynamo_table_name,
            "dynamodb.throughput.read.percent": "1.0",
        },
    )
    return dyf


def main():
    try:
        new_df = spark.read.schema(file_schema()).parquet(
            f"s3://{args.silver_bucket_name}/uc_events/"
        )
        current_dynamodb_df = current_dynamodb_data().toDF()

        removed_events_df = current_dynamodb_df.join(
            new_df, on="event_id", how="left_anti"
        )

        print(f"Deleted events '{removed_events_df.count()}':")
        removed_events_df.show()

        with dynamo_table.batch_writer() as batch:
            for row in removed_events_df.collect():
                batch.delete_item(Key={"event_id": row.event_id})

        dyf = DynamicFrame.fromDF(new_df, glue_context, "events_dyf")

        glue_context.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": args.dynamo_table_name,
                "dynamodb.throughput.write.percent": "1.0",
            },
        )

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
