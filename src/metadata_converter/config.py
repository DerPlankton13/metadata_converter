"""Loads a TOML config file and validates it as the right per-source config type.

Composes the three source-specific ``config.py`` modules (fetch/load) and the
uplift package's config into the discriminated unions used by the CLI
(``parse.py``). Each phase maps to exactly one config model or union, so the phase
itself does the discriminating between generic and source-specific uplift.
"""
from __future__ import annotations

import logging
import tomllib
from typing import Annotated, Union

from pydantic import Field, TypeAdapter, ValidationError

from metadata_converter.api_fetching.config import ApiFetchingConfig
from metadata_converter.biosamples.config import (
    BiosamplesConfig,
    BiosamplesUpliftRecordConfig,
)
from metadata_converter.flat_data.config import FlatDataConfig
from metadata_converter.uplift.config import GenericUpliftConfig

# Discriminated union of the three data-source config types.
# Used by load_source_config for the fetch and load phases.
SourceConfig = Annotated[
    Union[FlatDataConfig, ApiFetchingConfig, BiosamplesConfig],
    Field(discriminator="source_type"),
]

source_config_adapter: TypeAdapter[SourceConfig] = TypeAdapter(SourceConfig)

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


def load_uplift_config(path: str) -> GenericUpliftConfig:
    """Load and validate a generic uplift config from a TOML file.

    An ``uplift_record`` config is rejected here: ``GenericUpliftConfig`` forbids extra
    keys, so its ``source_type`` fails validation. The phase, not a union, decides which
    model a config is read as.
    """
    config = load_toml(path)
    try:
        return GenericUpliftConfig.model_validate(config)
    except ValidationError as e:
        handle_validation_error(e)


def load_uplift_record_config(path: str) -> BiosamplesUpliftRecordConfig:
    """Load and validate a source-specific ``uplift_record`` config from a TOML file.

    Biosamples is the only source with an ``uplift_record`` phase. If a second one
    appears, this becomes a discriminated union on ``source_type``, like
    ``SourceConfig``.
    """
    config = load_toml(path)
    try:
        return BiosamplesUpliftRecordConfig.model_validate(config)
    except ValidationError as e:
        handle_validation_error(e)
