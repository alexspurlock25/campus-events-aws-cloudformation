import os
import sys
from dataclasses import dataclass

import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window

try:
    from awsglue.utils import getResolvedOptions
except Exception:
    getResolvedOptions = None

s3_client = boto3.client("s3")


@dataclass
class Args:
    """
    Data type for args coming from the glue job
    """

    silver_bucket_name: str
    dynamo_table_name: str


if getResolvedOptions:
    _args = getResolvedOptions(
        sys.argv,
        [
            "SILVER_BUCKET",
            "DYNAMO_TABLE",
        ],
    )

    args = Args(
        silver_bucket_name=_args["SILVER_BUCKET"],
        dynamo_table_name=_args["DYNAMO_TABLE"],
    )
else:
    args = Args(
        silver_bucket_name=os.environ.get("SILVER_S3_PATH", "test-source-bucket"),
        dynamo_table_name=os.environ.get("DYNAMO_TABLE", "test-events-table"),
    )

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session


def main():
    window = Window.partitionBy("event_id").orderBy(desc("load_date"))
    df = spark.read.parquet(args.silver_bucket_name)
    events_df = (
        df.select(
            col("event_id"),
            col("title"),
            col("host"),
            col("start_date"),
            col("start_time"),
            col("end_date"),
            col("end_time"),
            col("location"),
            col("link"),
            col("categories"),
            col("record_source"),
            col("load_date"),
        )
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    dyf = glue_context.create_dynamic_frame.from_df(
        events_df, glue_context, "events_dyf"
    )

    glue_context.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="dynamodb",
        connection_options={
            "dynamodb.output.tableName": args.dynamo_table_name,
            "dynamodb.throughput.write.percent": "1.0",
        },
    )


if __name__ == "__main__":
    main()
