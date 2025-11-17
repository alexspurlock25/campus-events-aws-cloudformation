from lib.fetch_new_events import parse_rss


def test_parse_rss():
    """
    This makes a real network call. I will fix this later with a mock.
    """
    events = parse_rss("https://campuslink.uc.edu/events.rss")

    assert isinstance(events, list), "The returned events must be of type list."
    assert len(events) > 0, "We always expect something to return from the rss feed."
