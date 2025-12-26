import csv
import logging
from dataclasses import asdict, fields
from io import StringIO
from pathlib import Path

from lib.scripts.transform.xml_to_csv import Event, events_to_csv, parse_rss

logger = logging.getLogger(__name__)

FIXTURE = Path(__file__).parent.parent / "fixtures" / "events_20251211_060736.xml"


def test_parse_rss_given_xml_content_returning_events_list():
    xml_content = FIXTURE.read_text(encoding="utf-8")
    events = parse_rss(content=xml_content)
    assert isinstance(events, list)  # make sure the return is a list
    event = events[0]

    logger.info("Parsing event: %r", event)

    # test for fields
    assert isinstance(event.event_id, str)
    assert isinstance(event.title, str)
    assert isinstance(event.host, str)
    assert isinstance(event.start_date, str)
    assert isinstance(event.end_date, str)
    assert isinstance(event.start_time, str)
    assert isinstance(event.end_time, str)
    assert isinstance(event.event_description, str)
    assert isinstance(event.location, str)

    assert isinstance(event.link, str)
    assert event.link.startswith("https://campuslink.uc.edu/event/")

    assert isinstance(event.categories, list)
    assert all(isinstance(c, str) for c in event.categories)


def test_events_to_csv_round_trip_detects_column_shift():
    events = parse_rss(FIXTURE.read_text())
    csv_out = events_to_csv(events, FIXTURE.name)

    logger.info("csv_out: %s", csv_out)

    reader = csv.DictReader(StringIO(csv_out))
    rows = list(reader)

    # If a comma broke the CSV, DictReader will misalign keys
    row = rows[0]

    assert set(row.keys()) == {
        "record_source",
        "load_date",
        "event_id",
        "title",
        "host",
        "start_date",
        "end_date",
        "start_time",
        "end_time",
        "event_description",
        "location",
        "link",
        "categories",
    }
