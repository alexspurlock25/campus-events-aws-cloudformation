import logging
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
import feedparser
import pandas as pd
from awsglue.context import GlueContext
from awsglue.job import Job
from bs4 import BeautifulSoup
from feedparser import FeedParserDict

# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

try:
    from awsglue.utils import getResolvedOptions
except Exception:
    getResolvedOptions = None

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark_session = glue_context.spark_session
job = Job(glue_context)

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


#########################
# Types
#########################


@dataclass
class Args:
    """
    Data type for args coming from the glue job
    """

    job_name: str
    source_bucket_name: str
    target_bucket_name: str


@dataclass
class Event:
    event_id: int
    title: str
    host: str
    start_date_year: int
    start_date_month: int
    start_date_day: int
    end_date: str
    start_time: str
    end_time: str
    event_description: str
    location: str
    external_link: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "title": self.title,
            "host": self.host,
            "start_date_year": self.start_date_year,
            "start_date_month": self.start_date_month,
            "start_date_day": self.start_date_day,
            "end_date": self.end_date,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "event_description": self.event_description,
            "location": self.location,
            "external_link": self.external_link,
        }


if getResolvedOptions:
    _args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "SOURCE_BUCKET_NAME",
            "TARGET_BUCKET_NAME",
        ],
    )

    args = Args(
        job_name=_args["JOB_NAME"],
        source_bucket_name=_args["SOURCE_BUCKET_NAME"],
        target_bucket_name=_args["TARGET_BUCKET_NAME"],
    )

    job.init(args.job_name, _args)
else:
    args = Args(
        job_name=os.environ.get("JOB_NAME", "local-job"),
        source_bucket_name=os.environ.get("SOURCE_BUCKET_NAME", "test-source-bucket"),
        target_bucket_name=os.environ.get("TARGET_BUCKET_NAME", "test-target-bucket"),
    )

# After imports, before s3_client creation
IS_LOCAL = args.job_name.startswith("local")

#########################
# Helper functions
#########################

date_pattern = r"(\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4})"
time_pattern = r"(\d{2}:\d{2}:\d{2})"


def create_s3_client():
    if IS_LOCAL:
        logger.info("Running in LOCAL mode - using filesystem instead of S3")

        class LocalS3Client:
            def get_paginator(self, operation):
                class Paginator:
                    def paginate(self, Bucket):
                        import glob

                        files = glob.glob(f"{Bucket}/**/*.xml", recursive=True)
                        keys = [f.replace(f"{Bucket}/", "") for f in files]
                        yield {
                            "Contents": [
                                {"Key": k} for k in keys if not k.endswith("/")
                            ]
                        }

                return Paginator()

            def get_object(self, Bucket, Key):
                filepath = f"{Bucket}/{Key}"
                with open(filepath, "rb") as f:
                    body_content = f.read()

                class Body:
                    def __init__(self, content):
                        self.content = content

                    def read(self):
                        return self.content

                return {"Body": Body(body_content)}

            def copy_object(self, Bucket, Key, CopySource):
                src = f"{CopySource['Bucket']}/{CopySource['Key']}"
                dst = f"{Bucket}/{Key}"
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                import shutil

                shutil.copy(src, dst)

            def delete_object(self, Bucket, Key):
                filepath = f"{Bucket}/{Key}"
                if os.path.exists(filepath):
                    os.remove(filepath)

        return LocalS3Client()
    else:
        return boto3.client("s3")


def extract_description(entry: FeedParserDict) -> str:
    try:
        html = str(entry.get("description", ""))
        soup = BeautifulSoup(html, "html.parser")
        desc = soup.select_one(".p-description")
        if desc is None:
            return ""

        # Preserve sentence spacing, but no layout noise
        text = desc.get_text(separator=" ", strip=True)

        return unicodedata.normalize("NFKC", text)
    except Exception:
        return ""


def get_digits_from_guid(guid: str) -> int:
    guid_only = guid.rsplit("/")[-1]
    return int(guid_only)


def extract_campus(s3_key: str) -> str:
    """
    s3_key example: university-of-cincinnati/events_20251210_063602.xml
    """
    campus = s3_key.rsplit("/")[0]
    logger.info(f"extract_campus: s3_key={s3_key}, campus={campus}")
    return campus


def file_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("host", StringType(), True),
            StructField("start_date_year", IntegerType(), False),
            StructField("start_date_month", IntegerType(), False),
            StructField("start_date_day", IntegerType(), False),
            StructField("end_date", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("end_time", StringType(), False),
            StructField("event_description", StringType(), False),
            StructField("location", StringType(), False),
            StructField("external_link", StringType(), False),
            StructField("rss_source_campus", StringType(), False),
            StructField("record_source", StringType(), False),
            StructField("load_date", StringType(), False),
        ]
    )


#########################
# Main functions
#########################


def get_raw_keys() -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=args.source_bucket_name):
        for obj in page.get("Contents", []):
            file = obj.get("Key")
            if file and not file.endswith("/"):
                keys.append(file)
    return keys


def parse_rss(xml_byte_content: str) -> List[Event]:
    logger.info("Parsing content...")
    events: list[Event] = []
    feed = feedparser.parse(xml_byte_content)

    target_date_format = "%Y-%m-%d"

    for entry in feed.entries:
        title = str(entry["title"]).strip()
        event_id = get_digits_from_guid(guid=str(entry["guid"]).strip())
        host = ""
        location = ""
        link = ""
        if "host" in entry:
            host = str(entry["host"]).strip()
            location = re.sub(r"[^\x00-\x7F]+", " ", str(entry["location"])).strip()
            link = str(entry["link"]).strip()

        start_date_match = re.search(date_pattern, str(entry["start"]))
        start_date = ""
        if start_date_match is not None:
            start_date = datetime.strptime(
                start_date_match.group(0), "%d %b %Y"
            ).strftime(target_date_format)
        else:
            logger.warning(f"Start date was missing for event: {event_id}.")
            start_date = datetime.now(tz=timezone.utc).strftime(target_date_format)
            logger.warning(f"Using today's date: {start_date}")

        end_date_match = re.search(date_pattern, str(entry["end"]))
        end_date = ""
        if end_date_match is not None:
            end_date = datetime.strptime(end_date_match.group(0), "%d %b %Y").strftime(
                target_date_format
            )

        start_time_match = re.search(time_pattern, str(entry["start"]))
        start_time = ""
        if start_time_match is not None:
            start_time = start_time_match.group(0)

        end_time_match = re.search(time_pattern, str(entry["end"]))
        end_time = ""
        if end_time_match is not None:
            end_time = end_time_match.group(0)

        event_description = extract_description(entry)

        date_parts = start_date.split("-")

        event = Event(
            event_id=event_id,
            title=title,
            host=host,
            start_date_year=int(date_parts[0]),
            start_date_month=int(date_parts[1]),
            start_date_day=int(date_parts[2]),
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            event_description=event_description,
            location=location,
            external_link=link,
        )
        events.append(event)

    logger.info("Parsing complete.")
    return events


def events_to_dataframe(events: list[Event], s3_key: str) -> pd.DataFrame:
    """
    key example: university-of-cincinnati/events_20251210_063602.xml
    """
    logger.info(f"Converting events to spark dataframe for {s3_key}")
    load_date = datetime.now(tz=timezone.utc).isoformat()
    rows: List[Dict[str, Any]] = []
    _, filename = s3_key.rsplit("/", 1)

    for event in events:
        row: Dict[str, Any] = event.to_dict()
        row["rss_source_campus"] = extract_campus(s3_key)
        row["record_source"] = filename
        row["load_date"] = load_date
        rows.append(row)

    spark_df = spark_session.createDataFrame(
        [Row(**r) for r in rows], schema=file_schema()
    )
    logger.info("Spark dataframe created with given schema.")
    return spark_df


def main():
    s3_client = create_s3_client()
    raw_keys = get_raw_keys()
    if not raw_keys:
        logger.warning("No files to process")
        return

    for key in raw_keys:
        if "/processed/" in key:
            logger.warning(f"Skipping processed key: {key}")
            continue

        if not key.endswith(".xml"):
            logger.warning(f"Skipping non-xml key: {key}")
            continue

        try:
            logger.info(f"Staring to process s3://{args.source_bucket_name}/{key}")
            # example
            # prefix: university-of-cincinnati, filename=events_20251210_063602.xml, base=events_20251210_063602
            # csv_key: university-of-cincinnati/processed/events_20251210_063602.csv

            prefix, filename = key.rsplit("/", 1)
            base = filename.rsplit(".", 1)[0]
            logger.debug(f"prefix: {prefix}")
            logger.debug(f"filename: {filename} | base: {base}")

            obj = s3_client.get_object(Bucket=args.source_bucket_name, Key=key)
            raw_bytes = obj["Body"].read()
            xml_content = raw_bytes.decode("utf-8", errors="strict")
            events = parse_rss(xml_byte_content=xml_content)
            df = events_to_dataframe(events=events, s3_key=key)

            if IS_LOCAL:
                output_path = f"file://{args.target_bucket_name}/"
            else:
                output_path = f"s3://{args.target_bucket_name}/"

            # step 1: upload csv to staging
            df.write.partitionBy(
                "rss_source_campus",
                "start_date_year",
                "start_date_month",
                "start_date_day",
            ).mode("append").parquet(output_path)

            df.show(5)

            logger.info(f"Wrote partitioned parquet to {output_path}")

            # step 2: copy csv to /processed/ in raw bucket
            dest_key = f"{prefix}/processed/{filename}"
            s3_client.copy_object(
                Bucket=args.source_bucket_name,
                Key=dest_key,
                CopySource={
                    "Bucket": args.source_bucket_name,
                    "Key": key,
                },
            )

            # step 3: remove original file from raw bucket
            s3_client.delete_object(Bucket=args.source_bucket_name, Key=key)
            logger.info(
                f"moved s3://{args.source_bucket_name}/{key} -> s3://{args.source_bucket_name}/{dest_key}"
            )
        except Exception as e:
            logger.error(f"Failed to process key: {key}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
