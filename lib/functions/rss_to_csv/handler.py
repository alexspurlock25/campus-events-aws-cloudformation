'''
Lambda hanlder to convert rss data to csv
'''

import os
import json
import csv
import re
from io import StringIO
from datetime import datetime, timezone
from dataclasses import fields, asdict
from aws_cdk import aws_s3 as s3
import boto3
import feedparser
from bs4 import BeautifulSoup


@dataclass
class Event:
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


date_pattern = r"(\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4})"
time_pattern = r"(\d{2}:\d{2}:\d{2})"

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    '''
    Lambda hanlder to convert rss data to csv
    '''

    rss_url = os.environ["RSS_FEED_URL"]
    rss_feed_name = os.environ["RSS_FEED_NAME"]
    bucket_name = os.environ["CSV_BUCKET_NAME"]
    environment_name = os.environ["ENVIRONMENT_NAME"]

    try:
        events = parse_rss(url=rss_url)
        csv_out = events_to_csv(events=events)

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        s3_key = f"{environment_name}/{rss_feed_name}/{timestamp}.csv"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_out,
            ContentType='text/csv'
        )

        print(f"Successfully uploaded CSV to s3://{bucket_name}/{s3_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                'message': 'RSS feed processed successfully',
                's3_location': f"s3://{bucket_name}/{s3_key}",
                'items_processed': len(events)
            }),
        }
    except Exception as e:
        print(f"Error processing RSS feed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def parse_rss(url: str) -> list[Event]:
    events: list[Event] = []
    feed = feedparser.parse(url)

    for entry in feed.entries:
        categories: list[str] = []
        for tag in entry.tags:
            categories.append(str(tag.term))

        title = str(entry["title"]).strip()
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


def events_to_csv(events: list[Event]):
    output = StringIO()
    field_names = [f.name for f in fields(Event)]

    writer = csv.DictWriter(output, fieldnames=field_names)
    writer.writeheader()

    for event in events:
        writer.writerow(asdict(event))

    return output.getvalue()

