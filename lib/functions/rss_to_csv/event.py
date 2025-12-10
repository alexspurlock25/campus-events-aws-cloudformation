from dataclasses import dataclass


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
