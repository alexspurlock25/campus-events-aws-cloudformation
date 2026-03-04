import os
import tomllib
from dataclasses import dataclass

import yaml


@dataclass
class RssFeedConfig:
    """
    This object represents the ```rss_feed``` section in the .yml config file.
    """

    url: str
    schedule_expression: str
    email: str


@dataclass
class ProjectTomlConfig:
    """
    This object represents the ```pyproject.toml```.
    More fields can be added but I only needed this one.
    """

    project_name: str


def load_projecttoml_config() -> ProjectTomlConfig:
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
        print("Something went wrong while reading your pyproject.py file.", e)
        raise


def load_environment_config() -> RssFeedConfig:
    """
    Load configution file based on given environment
    Possible values: 'prod' (only this for now)
    """

    path = os.path.join("lib", "config", "config.yml")

    try:
        with open(path, "r") as f:
            yml = yaml.safe_load(f)
            return RssFeedConfig(
                url=yml["url"],
                schedule_expression=yml["schedule_expression"],
                email=yml["email"],
            )
    except OSError as e:
        print("Configuration did not load.", e)
        raise
