"""DataFrame cleaning and reshaping for the flat_data ingest pipeline."""

import re

import pandas as pd
from nanoid import generate

from metadata_converter.config import CleaningConfig
from metadata_converter.flat_data.cleaning_plugin import CleaningPlugin


def _run_plugins(df: pd.DataFrame, plugins: list[CleaningPlugin]) -> pd.DataFrame:
    for plugin in plugins:
        df = plugin.run(df)
    return df


def _clean_string(value):
    """Collapse runs of whitespace to a single space; return non-strings unchanged."""
    if not isinstance(value, str):
        return value
    return re.sub(r"\s+", " ", value).strip()


def _strip_header_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = pd.Index([_clean_string(col) for col in df.columns])
    return df


def _strip_cell_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.map(_clean_string))
    return df


def _sentinels_to_na(df: pd.DataFrame, sentinels: list[str]) -> pd.DataFrame:
    """
    Replace all occurrences of sentinel values with ``pd.NA``.
    Sentinel values are user-defined strings that represent missing or
    empty data, such as ``"N/A"`` or ``"-"``.
    """
    return df.replace({s: pd.NA for s in sentinels})


def _placeholders_to_na(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
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
    df = _run_plugins(df, config.plugins)
    if config.strip_header_whitespace:
        df = _strip_header_whitespace(df)
    if config.strip_cell_whitespace:
        df = _strip_cell_whitespace(df)
    if config.sentinels_to_na:
        df = _sentinels_to_na(df, config.empty_sentinels)
    if config.placeholders_to_na:
        df = _placeholders_to_na(df, config.placeholder_pattern)
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


def add_id(data: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    """Generate a nanoid-based ``@id`` column for each row: ``<schema_type>_<nanoid>.jsonld``."""
    data["@id"] = [f"{schema_type}_{generate()}.jsonld" for _ in range(len(data))]
    return data
