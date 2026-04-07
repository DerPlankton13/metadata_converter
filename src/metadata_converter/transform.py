import re
from typing import Any

import pandas as pd
from nanoid import generate
from pydantic import ValidationError

from metadata_converter.cleaning_plugin import CleaningPlugin
from metadata_converter.config import CleaningConfig, Config
from metadata_converter.schema_org_models.schemaorg_models import (
    SchemaOrgBase,
    get_schema,
)


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


def combine_columns(df: pd.DataFrame, mapping: dict[str, Any]) -> pd.DataFrame:
    """Adds new combined columns to the dataframe"""
    for model, props in mapping.items():
        if type(props) is dict:
            for key, value in props.items():
                if "+" in value:
                    # adds the values from the columns
                    columns = [col.strip() for col in value.split("+")]
                    df[key] = df[columns].agg(" ".join, axis=1)
                    mapping[model][key] = key

    return df


def generate_schema_id(schema_type: str) -> str:
    """Generate a unique identifier for a schema instance."""
    unique_id = generate()
    return f"{schema_type}_{unique_id}"


def build_schema(row, schema_type: str, properties: dict):
    """
    Recursively build a schema.org object from a row and mapping definition.

    Parameters
    ----------
    row : pandas.Series
        A row from a pandas DataFrame (from `iterrows`).
    schema_type : str
        The schema.org type to instantiate.
    properties : dict
        Mapping of schema properties. Values can be either:
        - str: column name
        - dict: nested schema definition (must include "type")

    Returns
    -------
    SchemaOrgBase or None
        Instantiated schema object, or None if no valid properties were found.

    Raises
    ------
    KeyError
        If a referenced column does not exist in the DataFrame.
    """
    schema_properties = {}

    for prop, value in properties.items():
        # --- Case 1: simple field ---
        if isinstance(value, str):
            try:
                field_value = row[value]
            except KeyError:
                raise KeyError(
                    f"Column '{value}' not found in DataFrame. "
                    f"Available columns are {list(row.index)}."
                )

            if not pd.isna(field_value):
                schema_properties[prop] = field_value

        # --- Case 2: nested schema ---
        elif isinstance(value, dict):
            nested_type = value.get("type")
            if not nested_type:
                raise ValueError(f"Missing 'type' in nested schema for '{prop}'")

            nested_props = {k: v for k, v in value.items() if k != "type"}

            nested_obj = build_schema(row, nested_type, nested_props)

            if nested_obj is not None:
                schema_properties[prop] = nested_obj

    # ensure ID
    if "id" not in schema_properties:
        schema_properties["id"] = generate_schema_id(schema_type)

    try:
        schema_class = get_schema(schema_type)
        return schema_class(**schema_properties)
    except ValidationError as e:
        for err in e.errors():
            print(
                f"Could not create a class of {schema_type}:",
                err["msg"],
                err["loc"],
                "but input was:",
                err.get("input"),
            )
        print(f"The following properties were provided: {schema_properties}")
        return None


def extract_schemas(df: pd.DataFrame, config: Config) -> list[SchemaOrgBase]:
    """
    Convert a pandas DataFrame into a list of schema.org objects,
    including nested schemas.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data containing structured records.
    config : Config
        Configuration object containing schema mappings.

    Returns
    -------
    list of SchemaOrgBase
        A list of instantiated schema.org objects validated via Pydantic.
    """
    schemas = []

    for _, row in df.iterrows():
        for schema_type, properties in config.mapping.items():
            schema = build_schema(row, schema_type, properties)
            if schema is not None:
                schemas.append(schema)

    return schemas
