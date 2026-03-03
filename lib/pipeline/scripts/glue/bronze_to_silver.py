import logging
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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
from pyspark.sql import Row, Window
from pyspark.sql import functions as F

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


def get_field(entry: FeedParserDict, field_name: str) -> Optional[str]:
    value = entry.get(field_name)
    if value:
        return str(value).strip()
    return None


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
        if start_date_match:
            start_date = datetime.strptime(start_date_match.group(0), "%d %b %Y").date()
        else:
            logger.warning(f"Start date was missing for event: {event_id}.")
            raise ValueError(f"Start date was missing for event: {event_id}.")

        end_date_match = re.search(date_pattern, str(entry["end"]))
        if end_date_match:
            end_date = datetime.strptime(end_date_match.group(0), "%d %b %Y").date()

        start_time_match = re.search(time_pattern, str(entry["start"]))
        start_time = ""
        if start_time_match is not None:
            start_time = start_time_match.group(0)

        end_time_match = re.search(time_pattern, str(entry["end"]))
        end_time = ""
        if end_time_match is not None:
            end_time = end_time_match.group(0)

        event_description = extract_description(entry)

        events.append(
            Event(
                event_id=event_id,
                title=title,
                host=host,
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time,
                event_description=event_description,
                location=location,
                external_link=link,
            )
        )

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


def write_deduplicated(new_df, output_path):
    try:
        existing_df = spark_session.read.parquet(output_path)
        combined_df = existing_df.union(new_df)
    except Exception as e:
        if "Path does not exist" in str(e) or "NoSuchKey" in str(e):
            logger.info("No existing data found, treating as first run.")
            combined_df = new_df
        else:
            logger.error(f"Unexpected error reading existing parquet: {e}")
            raise

    window = Window.partitionBy("event_id").orderBy(F.desc("record_source"))
    deduped_df = (
        combined_df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    active_ids = new_df.select("event_id").distinct()
    deduped_df = deduped_df.join(active_ids, on="event_id", how="inner")

    deduped_df.write.mode("overwrite").parquet(output_path)


def main():
    try:
        logger.info(
            f"Staring to process s3://{args.source_bucket_name}/{args.unprocessed_source_key}"
        )

        logger.debug(f"filename: {args.unprocessed_source_key}")

        obj = s3_client.get_object(
            Bucket=args.source_bucket_name, Key=args.unprocessed_source_key
        )
        raw_bytes = obj["Body"].read()
        xml_content = raw_bytes.decode("utf-8", errors="strict")
        events = parse_rss(xml_byte_content=xml_content)
        df = events_to_dataframe(events=events, s3_key=args.unprocessed_source_key)
        output_path = f"s3://{args.target_bucket_name}/uc_events/"

        write_deduplicated(new_df=df, output_path=output_path)
        logger.info(f"Wrote partitioned parquet to {output_path}")
    except Exception as e:
        logger.error(
            f"Failed to process key: {args.unprocessed_source_key} {e}", exc_info=True
        )
        raise Exception(f"Failed to process key: {args.unprocessed_source_key} {e}")


if __name__ == "__main__":
    main()
