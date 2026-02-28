"""
A module that contains data types for this project
"""

from dataclasses import dataclass
from typing import Any, Dict

from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@dataclass
class Event:
    event_id: int
    title: str
    host: str
    start_date_year: int
    start_date_month: int
    start_date_day: int
    end_date: str
    start_time: str
    end_time: str
    event_description: str
    location: str
    external_link: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "title": self.title,
            "host": self.host,
            "start_date_year": self.start_date_year,
            "start_date_month": self.start_date_month,
            "start_date_day": self.start_date_day,
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
            StructField("host", StringType(), True),
            StructField("start_date_year", IntegerType(), False),
            StructField("start_date_month", IntegerType(), False),
            StructField("start_date_day", IntegerType(), False),
            StructField("end_date", StringType(), False),
            StructField("start_time", StringType(), False),
            StructField("end_time", StringType(), False),
            StructField("event_description", StringType(), False),
            StructField("location", StringType(), False),
            StructField("external_link", StringType(), False),
            StructField("record_source", StringType(), False),
            StructField("load_date", StringType(), False),
        ]
    )
