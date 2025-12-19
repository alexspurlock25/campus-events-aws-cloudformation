import os
import tomllib
from dataclasses import dataclass
from typing import Literal, Optional

import yaml


@dataclass
class RssFeedConfig:
    """
    This object represents the ```rss_feed``` section in the .yml config file.
    """

    name: str
    url: str
    schedule_expression: str


@dataclass
class PipelineConfig:
    environment: str
    rss_feed: RssFeedConfig


@dataclass
class ProjectTomlConfig:
    """
    This object represents the ```pyproject.toml```.
    More fields can be added but I only needed this one.
    """

    project_name: str


def load_projecttoml_config() -> Optional[ProjectTomlConfig]:
    """
    Load the ```pyproject.toml``` file into ```ProjectTomlConfig``` dataclass.

    Right now, only the project name is being set but more fields can be added.
    """
    try:
        with open("pyproject.toml", "rb") as f:
            config_dict = tomllib.load(f)
            project_section = config_dict.get("project", {})
            return ProjectTomlConfig(project_name=project_section["name"])
    except OSError as e:
        return None


def load_environment_config(environment: Literal["prod"]) -> Optional[PipelineConfig]:
    """
    Load configution file based on given environment
    Possible values: 'prod' (only this for now)
    """

    path = os.path.join("lib", "config", "environments", f"{environment}.yml")

    try:
        with open(path, "r") as f:
            _config_dict = yaml.safe_load(f)

            rss_feed = RssFeedConfig(**_config_dict["rss_feed"])

            return PipelineConfig(
                environment=environment,
                rss_feed=rss_feed,
            )
    except OSError as e:
        # OSError is the base class for I/O errors so this
        # should catch any error relating to reading the config file
        return None
