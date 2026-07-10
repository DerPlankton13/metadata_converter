"""Derive new columns on wide-format DataFrames: combined columns and content-hash ``@id``."""

import logging

import pandas as pd

from metadata_converter.flat_data.config import FlatDataConfig
from metadata_converter.utils.hashing import content_hash

logger = logging.getLogger(__name__)


def add_columns(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, pd.DataFrame]:
    """Add configured combined columns and an ``@id`` column to every sheet."""
    new_data: dict[str, pd.DataFrame] = {}
    for name, data in data_dict.items():
        data = add_id(data, config.mapping[name]["type"])
        if combines := config.combined_columns.get(name):
            data = add_combined_columns(data, combines)
        new_data[name] = data
    return new_data


def add_combined_columns(
    data: pd.DataFrame, combines: dict[str, list[str]]
) -> pd.DataFrame:
    """Append new columns by joining non-NA source columns with a space.

    If all source values in a row are NA the combined column is also NA.
    ``{new_col: [sources]}``
    """
    for target_col, source_cols in combines.items():
        combined = data[source_cols].apply(
            lambda row: " ".join(str(v) for v in row if pd.notna(v)),
            axis=1,
        )
        data[target_col] = combined.replace("", pd.NA)
    return data


def add_id(data: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    """Generate a content-hash-based ``@id`` for each row: ``<schema_type>_<hash>.jsonld``.

    A content hash (rather than e.g. a random id) makes ``@id`` deterministic:
    the same real-world entity always receives the same ``@id``, regardless
    of how many input files it appears in or how many times the pipeline
    runs. This is what prevents duplicate entities (e.g. the same author
    appearing across several datasets) from being written as separate files
    and subsequently linked as spurious duplicates during uplifting.
    """
    data["@id"] = [row_id(row, schema_type) for _, row in data.iterrows()]
    return data


def row_id(row: pd.Series, schema_type: str) -> str:
    """Build the ``<schema_type>_<hash>.jsonld`` id for a single row.

    Drops NA values before hashing, since pandas represents missing values
    as ``NaN``/``NaT`` rather than ``None``.
    """
    non_null_values = {k: v for k, v in row.items() if pd.notna(v)}
    return f"{schema_type}_{content_hash(non_null_values)}.jsonld"
