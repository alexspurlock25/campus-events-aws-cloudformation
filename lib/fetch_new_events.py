import json
import os
import re
from datetime import datetime

import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from Event import Event
from supabase import Client, create_client

date_pattern = r"(\d{1,2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4})"
time_pattern = r"(\d{2}:\d{2}:\d{2})"


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


def add_entries(supabase: Client, events: list[Event]):
    unique_events = {event.title: event.to_dict() for event in events}
    main_list = list(unique_events.values())
    supabase.table("events").upsert(main_list).execute()


def lambda_handler(event, context):
    try:
        rss_url = os.environ.get("uc_events_rss_feed")
        sb_url = os.environ.get("supabase_url")
        sb_key = os.environ.get("supabase_key")
        if rss_url is not None and sb_url is not None and sb_key is not None:
            events = parse_rss(rss_url)
            supabase = create_client(sb_url, sb_key)
            add_entries(supabase, events)
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Entries Added!",
                }
            ),
        }
    except Exception as e:
        return {"statusCode": 500, "body": repr(e)}


def main():
    """
    This is here only for testing using the terminal. AWS Lambda will call the handler.
    """
    env_loaded = load_dotenv(dotenv_path="./.env.local")
    if env_loaded:
        lambda_handler(None, None)


if __name__ == "__main__":
    main()
