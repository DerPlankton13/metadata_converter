import logging
import re
from typing import Any

import pandas as pd
from nanoid import generate
from pydantic import ValidationError

from metadata_converter.config import CleaningConfig
from metadata_converter.flat_data.cleaning_plugin import CleaningPlugin
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import (
    SchemaOrgBase,
)

logger = logging.getLogger(__name__)


def _run_plugins(df: pd.DataFrame, plugins: list[CleaningPlugin]) -> pd.DataFrame:
    """
    Run all user-defined cleaning plugins in order, passing the dataframe
    through each plugin's ``run`` method sequentially.
    """
    for plugin in plugins:
        df = plugin.run(df)
    return df


def _clean_string(value) -> str | Any:
    """
    Normalize whitespace in a string value by replacing any sequence of
    whitespace characters — including spaces, tabs and newlines — with a
    single space, then removing leading and trailing whitespace.
    Returns ``value`` if the value is not a string.

    Note that ``\\s`` in the regex matches any whitespace character
    (space, tab, newline, carriage return), and the ``+`` quantifier
    means one or more consecutive whitespace characters are collapsed
    into a single space.
    """
    if not isinstance(value, str):
        return value
    return re.sub(r"\s+", " ", value).strip()


def _strip_header_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column headers by applying ``_clean_string`` to each
    header name.
    """
    df.columns = pd.Index([_clean_string(col) for col in df.columns])
    return df


def _strip_cell_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply ``_clean_string`` element-wise to all string (object dtype)
    columns in the dataframe.
    """
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
    """
    Apply a configurable sequence of cleaning steps to a dataframe.

    Cleaning steps are applied in the following order:

    1. User-defined plugins
    2. Strip header whitespace
    3. Strip cell whitespace
    4. Replace sentinels for missing data with ``pd.NA``
    5. Replace missing data ``pd.NA`` according to a regex pattern
    6. Infer best column dtypes
    7. Drop fully empty rows

    Parameters
    ----------
    df : pd.DataFrame
        The raw input dataframe to clean.
    config : CleaningConfig
        Configuration controlling which cleaning steps are applied
        and their parameters.

    Returns
    -------
    pd.DataFrame
        The cleaned dataframe with reset index.
    """
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

    # drop fully empty rows
    df.dropna(how="all", inplace=True)

    return df.reset_index(drop=True)


def combine_columns(df: pd.DataFrame, mapping_value: Any) -> None:
    """Add combined columns to ``df`` from ``"col1 + col2"`` mapping values.

    Mutates ``df`` in place (adds the new columns) and ``mapping_value`` in place
    (rewrites the ``"+"`` expression to the new column name so downstream code
    treats it as a normal column lookup). Recurses into nested dicts and lists so
    combinations work at any depth of the mapping.

    Parameters
    ----------
    df : pd.DataFrame
        Wide-format dataframe for a single sheet.
    mapping_value : Any
        A single sheet's mapping (dict), a nested mapping (dict), or a list of
        nested mappings. Strings and other scalars are ignored.
    """
    if isinstance(mapping_value, dict):
        for key, value in list(mapping_value.items()):
            if key == "type":
                continue
            if isinstance(value, str) and "+" in value:
                columns = [col.strip() for col in value.split("+")]
                df[key] = df[columns].agg(" ".join, axis=1)
                mapping_value[key] = key
            elif isinstance(value, (dict, list)):
                combine_columns(df, value)
    elif isinstance(mapping_value, list):
        for item in mapping_value:
            combine_columns(df, item)


def convert_to_long(df: pd.DataFrame, sheet_name: str = None) -> pd.DataFrame:
    """Converts the data into a long format"""
    df["id"] = df.index.astype(str)
    if sheet_name:
        df.id = sheet_name + "_" + df.id
    return df.melt(id_vars=["id"], var_name="header")


def add_id(data: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    """Generate a nanoid-based ``@id`` column for each row: ``<schema_type>_<nanoid>.jsonld``."""
    data["@id"] = [f"{schema_type}_{generate()}.jsonld" for _ in range(len(data))]
    return data
