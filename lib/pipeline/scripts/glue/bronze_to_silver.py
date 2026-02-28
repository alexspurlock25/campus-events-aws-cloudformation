import logging
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
from awsglue.utils import getResolvedOptions
from bs4 import BeautifulSoup
from ce_types import Event, file_schema
from feedparser import FeedParserDict
from pyspark.context import SparkContext
from pyspark.sql import Row

s3_client = boto3.client("s3")
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
    unprocessed_source_key: str
    source_bucket_name: str
    target_bucket_name: str


_args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "UNPROCESSED_SOURCE_KEY",
        "SOURCE_BUCKET_NAME",
        "TARGET_BUCKET_NAME",
    ],
)

args = Args(
    job_name=_args["JOB_NAME"],
    unprocessed_source_key=_args["UNPROCESSED_SOURCE_KEY"],
    source_bucket_name=_args["SOURCE_BUCKET_NAME"],
    target_bucket_name=_args["TARGET_BUCKET_NAME"],
)

job.init(args.job_name, _args)

#########################
# Helper functions
#########################

date_pattern = r"(\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4})"
time_pattern = r"(\d{2}:\d{2}:\d{2})"


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
    try:
        guid_only = guid.rsplit("/")[-1]
        return int(guid_only)
    except ValueError:
        raise Exception(f"Invalid GUID: {guid}")


def get_field(entry: FeedParserDict, field_name: str) -> str:
    value = entry.get(field_name) or ""
    return str(value).strip()


def parse_rss(xml_byte_content: str) -> List[Event]:
    logger.info("Parsing content...")
    events: List[Event] = []
    feed = feedparser.parse(xml_byte_content)

    target_date_format = "%Y-%m-%d"

    for entry in feed.entries:
        title = str(entry["title"]).strip()
        event_id = get_digits_from_guid(guid=str(entry["guid"]).strip())
        host = get_field(entry, "host")
        location = re.sub(
            r"[^\x00-\x7F]+", " ", str(entry["location"] if entry["location"] else "")
        ).strip()
        link = get_field(entry, "link")

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
    key example: new/events_20251210_063602.xml
    """
    logger.info(f"Converting events to spark dataframe for {s3_key}")
    load_date = datetime.now(tz=timezone.utc).isoformat()
    rows: List[Dict[str, Any]] = []
    _, filename = s3_key.rsplit("/", 1)

    for event in events:
        row: Dict[str, Any] = event.to_dict()
        row["record_source"] = filename
        row["load_date"] = load_date
        rows.append(row)

    spark_df = spark_session.createDataFrame(
        [Row(**r) for r in rows], schema=file_schema()
    )
    logger.info("Spark dataframe created with given schema.")
    return spark_df


def main():
    try:
        logger.info(
            f"Staring to process s3://{args.source_bucket_name}/{args.unprocessed_source_key}"
        )

        _, filename = args.unprocessed_source_key.rsplit("/", 1)
        logger.debug(f"filename: {filename}")

        obj = s3_client.get_object(
            Bucket=args.source_bucket_name, Key=args.unprocessed_source_key
        )
        raw_bytes = obj["Body"].read()
        xml_content = raw_bytes.decode("utf-8", errors="strict")
        events = parse_rss(xml_byte_content=xml_content)
        df = events_to_dataframe(events=events, s3_key=args.unprocessed_source_key)

        output_path = f"s3://{args.target_bucket_name}/ce_events/"

        # step 1: upload csv to staging
        df.write.partitionBy(
            "start_date_year",
            "start_date_month",
            "start_date_day",
        ).mode("append").parquet(output_path)

        df.show(3)

        logger.info(f"Wrote partitioned parquet to {output_path}")

        # step 2: copy csv to /processed/ in raw bucket
        dest_key = f"processed/{filename}"
        s3_client.copy_object(
            Bucket=args.source_bucket_name,
            Key=dest_key,
            CopySource={
                "Bucket": args.source_bucket_name,
                "Key": args.unprocessed_source_key,
            },
        )

        # step 3: remove original file from raw bucket
        s3_client.delete_object(
            Bucket=args.source_bucket_name, Key=args.unprocessed_source_key
        )
        logger.info(
            f"moved s3://{args.source_bucket_name}/{args.unprocessed_source_key} -> s3://{args.source_bucket_name}/{dest_key}"
        )
    except Exception as e:
        logger.error(
            f"Failed to process key: {args.unprocessed_source_key} {e}", exc_info=True
        )
        raise Exception(f"Failed to process key: {args.unprocessed_source_key} {e}")


if __name__ == "__main__":
    main()
