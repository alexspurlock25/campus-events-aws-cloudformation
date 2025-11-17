class Event:
    def __init__(
        self,
        title: str,
        host: str,
        start_date: str,
        end_date: str,
        start_time: str,
        end_time: str,
        event_description: str,
        location: str,
        link: str,
        categories: list[str],
    ) -> None:
        self.title: str = title
        self.host: str = host
        self.start_date: str = start_date
        self.end_date: str = end_date
        self.start_time: str = start_time
        self.end_time: str = end_time
        self.event_description: str = event_description
        self.location: str = location
        self.link: str = link
        self.categories: list[str] = categories

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
