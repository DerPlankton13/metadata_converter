"""Derive new columns on wide-format DataFrames: combined columns and content-hash ``@id``."""

import base64
import hashlib
import json
import logging

import pandas as pd

from metadata_converter.config import FlatDataConfig

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
    df: pd.DataFrame, combines: dict[str, list[str]]
) -> pd.DataFrame:
    """Append new columns by joining non-NA source columns with a space.

    If all source values in a row are NA the combined column is also NA.
    ``{new_col: [sources]}``
    """
    for target_col, source_cols in combines.items():
        combined = df[source_cols].apply(
            lambda row: " ".join(str(v) for v in row if pd.notna(v)),
            axis=1,
        )
        df[target_col] = combined.replace("", pd.NA)
    return df


def add_id(data: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    """Generate a content-hash-based ``@id`` for each row: ``<schema_type>_<hash>.jsonld``."""
    data["@id"] = [f"{schema_type}_{row_hash(row)}.jsonld" for _, row in data.iterrows()]
    return data


def row_hash(row: pd.Series) -> str:
    """Return a 22-character URL-safe base64 hash of the row's content.

    The hash is derived from the row's non-null values serialised as canonical
    JSON (keys sorted, non-standard types coerced to str). The first 22
    characters of the base64url-encoded SHA-256 digest are returned, encoding
    132 bits of entropy — negligible collision probability at any realistic
    dataset size.

    Using a content hash instead of a random ID makes ``@id`` deterministic:
    the same real-world entity always receives the same ``@id``, regardless of
    how many input files it appears in or how many times the pipeline runs.
    This is the mechanism that prevents duplicate entities (e.g. the same
    author appearing across several datasets) from being written as separate
    files and subsequently linked as spurious duplicates during uplifting.
    """
    row_dict = {k: v for k, v in row.items() if pd.notna(v)}
    canonical = json.dumps(row_dict, sort_keys=True, default=str)
    digest = hashlib.sha256(canonical.encode()).digest()
    return base64.urlsafe_b64encode(digest)[:22].decode()
