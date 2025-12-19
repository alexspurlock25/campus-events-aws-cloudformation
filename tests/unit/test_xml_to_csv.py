from pathlib import Path
from lib.scripts.transform.xml_to_csv import parse_rss, events_to_csv
from dataclasses import dataclass, fields

FIXTURE = Path(__file__).parent.parent / "fixtures" / "events_20251211_060736.xml"


def test_parse_rss_given_xml_content_returning_events_list():
    xml_content = FIXTURE.read_text(encoding="utf-8")
    events = parse_rss(content=xml_content)
    assert isinstance(events, list)  # make sure the return is a list
    event = events[0]
    # test for fields
    assert getattr(event, "event_id") != ""
    assert getattr(event, "title") != ""
    assert getattr(event, "host") != ""
    assert getattr(event, "start_date") != ""
    assert getattr(event, "end_date") != ""
    assert getattr(event, "start_time") != ""
    assert getattr(event, "end_time") != ""
    assert getattr(event, "event_description") != ""
    assert getattr(event, "location") != ""
    assert getattr(event, "link") != ""
    assert len(fields(event)) == 11  # at the time of creating the test


def test_events_to_csv_given_event_list_and_record_source_returning_csv_str_output():
    xml_content = FIXTURE.read_text(encoding="utf-8")
    events = parse_rss(content=xml_content)
    assert len(events) > 0
    csv_out = events_to_csv(events=events, filename=FIXTURE.name)
    lines = [l for l in csv_out.splitlines() if l.strip()]
    assert "record_source" in lines[0]
    assert "load_date" in lines[0]
    assert "event_id" in lines[0]
    assert "title" in lines[0]
    assert "host" in lines[0]
    assert "start_date" in lines[0]
    assert "end_date" in lines[0]
    assert "start_time" in lines[0]
    assert "end_time" in lines[0]
    assert "event_description" in lines[0]
    assert "location" in lines[0]
    assert "link" in lines[0]
    assert "categories" in lines[0]
    assert len(lines) == 1 + len(events)
