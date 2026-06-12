"""DataFrame cleaning and reshaping for the flat_data ingest pipeline."""

import base64
import hashlib
import json
import re

import pandas as pd

from metadata_converter.config import CleaningConfig
from metadata_converter.flat_data.cleaning_plugin import CleaningPlugin


def run_plugins(df: pd.DataFrame, plugins: list[CleaningPlugin]) -> pd.DataFrame:
    for plugin in plugins:
        df = plugin.run(df)
    return df


def clean_string(value):
    """Collapse runs of whitespace to a single space; return non-strings unchanged."""
    if not isinstance(value, str):
        return value
    return re.sub(r"\s+", " ", value).strip()


def strip_header_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = pd.Index([clean_string(col) for col in df.columns])
    return df


def strip_cell_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.map(clean_string))
    return df


def sentinels_to_na(df: pd.DataFrame, sentinels: list[str]) -> pd.DataFrame:
    """
    Replace all occurrences of sentinel values with ``pd.NA``.
    Sentinel values are user-defined strings that represent missing or
    empty data, such as ``"N/A"`` or ``"-"``.
    """
    return df.replace({s: pd.NA for s in sentinels})


def placeholders_to_na(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
    """
    Replace cell values matching ``pattern`` with ``pd.NA`` in all string
    (object dtype) columns. Intended for bracketed placeholder values
    such as ``"[Please enter value]"``.
    """
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.where(~col.str.match(pattern, na=False), other=pd.NA)
    )
    return df


def clean_dataframe(df: pd.DataFrame, config: CleaningConfig) -> pd.DataFrame:
    """Apply configured cleaning steps in order: plugins → strip header/cell whitespace →
    replace sentinels/placeholders → infer dtypes → drop fully empty rows."""
    df = run_plugins(df, config.plugins)
    if config.strip_header_whitespace:
        df = strip_header_whitespace(df)
    if config.strip_cell_whitespace:
        df = strip_cell_whitespace(df)
    if config.sentinels_to_na:
        df = sentinels_to_na(df, config.empty_sentinels)
    if config.placeholders_to_na:
        df = placeholders_to_na(df, config.placeholder_pattern)
    df = df.convert_dtypes()
    df.dropna(how="all", inplace=True)
    return df.reset_index(drop=True)


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


def convert_to_long(df: pd.DataFrame, sheet_name: str = None) -> pd.DataFrame:
    """Melt wide-format DataFrame to (id, header, value) long format."""
    df["id"] = df.index.astype(str)
    if sheet_name:
        df.id = sheet_name + "_" + df.id
    return df.melt(id_vars=["id"], var_name="header")


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


def add_id(data: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    """Generate a content-hash-based ``@id`` for each row: ``<schema_type>_<hash>.jsonld``."""
    data["@id"] = [f"{schema_type}_{row_hash(row)}.jsonld" for _, row in data.iterrows()]
    return data
