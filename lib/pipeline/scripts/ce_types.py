"""
A module that contains data types for this project
"""

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType


@dataclass
class Event:
    event_id: int
    title: str
    host: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    event_description: Optional[str] = None
    location: Optional[str] = None
    external_link: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
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
            "external_link": self.external_link,
        }


def file_schema() -> StructType:
    """
    Returns a StructType representing the schema of the events file.
    """

    return StructType(
        [
            StructField("event_id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("host", StringType()),
            StructField("start_date", DateType()),
            StructField("end_date", DateType()),
            StructField("start_time", StringType()),
            StructField("end_time", StringType()),
            StructField("event_description", StringType()),
            StructField("location", StringType()),
            StructField("external_link", StringType()),
            StructField("record_source", StringType(), False),
            StructField("load_date", StringType(), False),
        ]
    )
