"""Clean wide-format DataFrames: strip whitespace, normalize sentinels, run plugins."""

import logging
import re

import pandas as pd

from metadata_converter.config import CleaningConfig, FlatDataConfig
from metadata_converter.flat_data.transform.cleaning_plugin import CleaningPlugin

logger = logging.getLogger(__name__)


def clean(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, pd.DataFrame]:
    """Apply configured cleaning to every sheet."""
    new_data: dict[str, pd.DataFrame] = {}
    for name, data in data_dict.items():
        logger.info("Cleaning sheet '%s'", name)
        new_data[name] = clean_dataframe(data, config.cleaning)
    return new_data


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
    """Replace all occurrences of sentinel values with ``pd.NA``."""
    return df.replace({s: pd.NA for s in sentinels})


def placeholders_to_na(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
    """Replace cell values matching ``pattern`` with ``pd.NA`` in string columns."""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.where(~col.str.match(pattern, na=False), other=pd.NA)
    )
    return df
