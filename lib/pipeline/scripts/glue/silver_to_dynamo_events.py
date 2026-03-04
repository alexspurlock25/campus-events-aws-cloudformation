import sys
from dataclasses import dataclass

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from uc_types import file_schema

s3_client = boto3.client("s3")


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

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session


def main():
    try:
        df = spark.read.schema(file_schema()).parquet(
            f"s3://{args.silver_bucket_name}/uc_events/"
        )

        dyf = DynamicFrame.fromDF(df, glue_context, "events_dyf")

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
