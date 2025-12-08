import os
from dataclasses import dataclass
from typing import Optional, Literal
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


def load_config(environment: Literal["Prod"]) -> Optional[PipelineConfig]:
    """
    Load configution file based on given environment
    Possible values: 'Prod' (only this for now)
    """

    path = os.path.join("lib", "config", "environments", f"{environment}.yml")

    try:
        with open(path, "r") as f:
            _config_dict = yaml.safe_load(f)

            rss_feed = RssFeedConfig(**_config_dict["rss_feed"])

            return PipelineConfig(
                environment=environment,
                rss_feed=rss_feed,
                csv_bucket_name=_config_dict["csv_bucket_name"],
            )
    except OSError as e:
        # OSError is the base class for I/O errors so this
        # should catch any error relating to reading the config file
        return None
