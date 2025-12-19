import os
import csv
import re
import sys
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from importlib.metadata import metadata
from io import StringIO

import boto3
import feedparser
from bs4 import BeautifulSoup

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

    job_name: str
    source_bucket_name: str
    target_bucket_name: str


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
else:
    args = Args(
        job_name=os.environ.get("JOB_NAME", "local-job"),
        source_bucket_name=os.environ.get("SOURCE_BUCKET_NAME", "test-source-bucket"),
        target_bucket_name=os.environ.get("TARGET_BUCKET_NAME", "test-target-bucket"),
    )

date_pattern = r"(\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4})"
time_pattern = r"(\d{2}:\d{2}:\d{2})"


@dataclass
class Event:
    event_id: str
    title: str
    host: str
    start_date: str
    end_date: str
    start_time: str
    end_time: str
    event_description: str
    location: str
    link: str
    categories: list[str]

    def to_dict(self):
        return {
            "event_id": self.event_id,
            "title": self.title,
            "host": self.host,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "event_description": self.event_description,
            "location": self.location,
            "external_link": self.link,
            "categories": self.categories,
        }


def parse_rss(content: str) -> list[Event]:
    events: list[Event] = []
    feed = feedparser.parse(content)

    for entry in feed.entries:
        categories: list[str] = []
        for tag in entry.tags:
            categories.append(str(tag.term))

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
            ).strftime("%Y-%m-%d")

        end_date_match = re.search(date_pattern, str(entry["end"]))
        end_date = ""
        if end_date_match is not None:
            end_date = datetime.strptime(end_date_match.group(0), "%d %b %Y").strftime(
                "%Y-%m-%d"
            )

        start_time_match = re.search(time_pattern, str(entry["start"]))
        start_time = ""
        if start_time_match is not None:
            start_time = start_time_match.group(0)

        end_time_match = re.search(time_pattern, str(entry["end"]))
        end_time = ""
        if end_time_match is not None:
            end_time = end_time_match.group(0)

        stripped_html = BeautifulSoup(str(entry["summary"]), "html.parser").get_text(
            strip=True
        )
        event_description = re.sub(r"[^\x00-\x7F]+", " ", stripped_html)

        event = Event(
            event_id,
            title,
            host,
            start_date,
            end_date,
            start_time,
            end_time,
            event_description,
            location,
            link,
            categories,
        )
        events.append(event)
    return events


def get_digits_from_guid(guid: str) -> str:
    return guid.rsplit("/")[-1]


def events_to_csv(events: list[Event], filename: str) -> str:
    output = StringIO()
    base_fields = [f.name for f in fields(Event)]
    metadata_fields = ["record_source", "load_date"]
    field_names = metadata_fields + base_fields

    writer = csv.DictWriter(output, fieldnames=field_names)
    writer.writeheader()

    load_date = datetime.now(tz=timezone.utc).isoformat()

    for event in events:
        row = asdict(event)
        row["record_source"] = filename
        row["load_date"] = load_date

        writer.writerow(row)

    return output.getvalue()


def get_raw_keys() -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=args.source_bucket_name):
        for obj in page.get("Contents", []):
            file = obj.get("Key")
            if file and not file.endswith("/"):
                keys.append(file)
    return keys


def main():
    raw_keys = get_raw_keys()
    if not raw_keys:
        print("no files to process!")

    for key in raw_keys:
        if "/processed/" in key:
            print(f"skipping processed key: {key}")
            continue

        if not key.endswith(".xml"):
            print(f"skipping non-xml key: {key}")
            continue

        try:
            print(f"processing s3://{args.source_bucket_name}/{key}")
            # example
            # prefix: university-of-cincinnati, filename=events_20251210_063602.xml, base=events_20251210_063602
            # csv_key: university-of-cincinnati/processed/events_20251210_063602.csv

            prefix, filename = key.rsplit("/", 1)
            base = filename.rsplit(".", 1)[0]
            print(f"prefix: {prefix}")
            print(f"filename: {filename} | base: {base}")

            obj = s3_client.get_object(Bucket=args.source_bucket_name, Key=key)
            raw_bytes = obj["Body"].read()
            xml_content = raw_bytes.decode("utf-8", errors="replace")
            events = parse_rss(content=xml_content)
            csv_events = events_to_csv(events=events, filename=filename)

            # step 1: upload csv to staging
            s3_client.put_object(
                Bucket=args.target_bucket_name,
                Key=f"{prefix}/{base}.csv",
                Body=csv_events.encode("utf-8"),
                ContentType="text/csv",
            )
            print(f"wrote s3://{args.target_bucket_name}/{prefix}/{base}.csv")

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
            print(
                f"moved s3://{args.source_bucket_name}/{key} -> s3://{args.source_bucket_name}/{dest_key}"
            )
        except Exception as e:
            print(f"there was an error while processing {key}, error: {e}")
            break


if __name__ == "__main__":
    main()
