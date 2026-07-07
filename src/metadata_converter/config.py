"""Loads a TOML config file and validates it as the right per-source config type.

Composes the three source-specific ``config.py`` modules (fetch/load) and the
uplift package's config into the discriminated unions used by the CLI
(``parse.py``).
"""
from __future__ import annotations

import logging
import tomllib
from typing import Annotated, Union

from pydantic import Field, TypeAdapter, ValidationError

from metadata_converter.api_fetching.config import ApiFetchingConfig
from metadata_converter.biosamples.config import BiosamplesConfig, BiosamplesUpliftConfig
from metadata_converter.flat_data.config import FlatDataConfig
from metadata_converter.uplift.config import GenericUpliftConfig

# Discriminated union of the three data-source config types.
# Used by load_source_config for the fetch and load phases.
SourceConfig = Annotated[
    Union[FlatDataConfig, ApiFetchingConfig, BiosamplesConfig],
    Field(discriminator="source_type"),
]

source_config_adapter: TypeAdapter[SourceConfig] = TypeAdapter(SourceConfig)

# Union of the two uplift config types — no discriminator field needed:
# BiosamplesUpliftConfig requires source_type (no default) and GenericUpliftConfig
# forbids it, so at most one member ever validates a given input. Used by
# load_uplift_config — exactly one uplift run per config.
UpliftConfig = Union[BiosamplesUpliftConfig, GenericUpliftConfig]

uplift_config_adapter: TypeAdapter[UpliftConfig] = TypeAdapter(UpliftConfig)

logger = logging.getLogger(__name__)


def load_toml(path: str) -> dict:
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        logger.error("Config file not found: %s", path)
        raise SystemExit(1)


def handle_validation_error(e: ValidationError) -> None:
    first = e.errors()[0]
    logger.error(
        "Invalid config — %s at %s (input was: %s)",
        first["msg"],
        first["loc"],
        first["input"],
    )
    raise SystemExit(1)


def load_source_config(
    path: str,
) -> FlatDataConfig | ApiFetchingConfig | BiosamplesConfig:
    """Load and validate a source config (flat_data, biosamples, or api) from a TOML file."""
    config = load_toml(path)
    try:
        return source_config_adapter.validate_python(config)
    except ValidationError as e:
        handle_validation_error(e)


def load_uplift_config(
    path: str,
) -> BiosamplesUpliftConfig | GenericUpliftConfig:
    """Load and validate an uplift config (biosamples-specific or generic) from a TOML file."""
    config = load_toml(path)
    try:
        return uplift_config_adapter.validate_python(config)
    except ValidationError as e:
        handle_validation_error(e)
