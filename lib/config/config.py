from dataclasses import dataclass
from typing import Any
import yaml


@dataclass
class RssFeedConfig:
    name: str
    url: str
    schedule_expression: str


@dataclass
class PipelineConfig:
    environment: str
    rss_feed: RssFeedConfig
    csv_bucket_name: str


def load_config(environment: str) -> PipelineConfig:
    """
    Load configution file based on given environment
    Possible values: 'prod' (only this for now)
    """

    config_dict: Any

    with open(f"./lib/config/environments/{environment}.yml", "r") as f:
        config_dict = yaml.safe_load(f)

    rss_feed = RssFeedConfig(**config_dict["rss_feed"])

    return PipelineConfig(
        environment=environment,
        rss_feed=rss_feed,
        csv_bucket_name=config_dict["csv_bucket_name"],
    )
