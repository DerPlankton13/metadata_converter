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


def _strip_header_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column headers by collapsing internal whitespace and
    removing leading and trailing whitespace and newlines.
    """
    df.columns = df.columns.str.replace(r"[\s\n]+", " ", regex=True).str.strip()
    return df


def _clean_string(value) -> str | None:
    """
    Collapse internal whitespace and strip leading and trailing
    whitespace from a single string value. Returns ``None`` if the
    value is ``NaN`` or missing.
    """
    if pd.isna(value):
        return None
    return re.sub(r"[\s\n]+", " ", str(value)).strip()


def _strip_cell_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply ``_clean_string`` element-wise to all string (object dtype)
    columns in the dataframe.
    """
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.map(_clean_string))
    return df


def _normalize_empty_to_nan(df: pd.DataFrame, sentinels: list[str]) -> pd.DataFrame:
    """
    Replace all occurrences of sentinel values with ``None``, which
    pandas treats as ``NaN``. Sentinel values are user-defined strings
    that represent missing or empty data, such as ``"N/A"`` or ``"-"``.
    """
    return df.replace({s: None for s in sentinels})


def _placeholders_to_nan(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
    """
    Replace cell values matching ``pattern`` with ``NaN`` in all string
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
    4. Normalize empty sentinels to ``NaN``
    5. Replace bracketed placeholders with ``NaN``
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

    if config.normalize_empty_to_nan:
        df = _normalize_empty_to_nan(df, config.empty_sentinels)

    if config.placeholders_to_nan:
        df = _placeholders_to_nan(df, config.placeholder_pattern)

    df = df.convert_dtypes()

    if config.drop_fully_empty_rows:
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


def extract_schemas(df: pd.DataFrame, config: Config) -> list[SchemaOrgBase]:

    schemas = []
    for _, row in df.iterrows():
        # go through all mappings defined in the toml
        for schema_type, properties in config.mapping.items():
            schema_properties = {}
            for prop, header in properties.items():
                # only add valid values to the dict
                if not pd.isna(row[header]):
                    schema_properties[prop] = row[header]
            # ensures that there is always an id
            if "id" not in schema_properties.keys():
                # todo: think about other ways to generate the id
                unique_id = generate()
                schema_properties["id"] = schema_type + f"_{unique_id}"
            try:
                schema = get_schema(schema_type)
                schemas.append(schema(**schema_properties))
            except ValidationError as e:
                print(
                    f"Could not create a class of {schema_type}:",
                    e.errors()[0]["msg"],
                    e.errors()[0]["loc"],
                    "but input was:",
                    e.errors()[0]["input"],
                    f"The following properties were provided: {schema_properties}",
                )
                continue
    return schemas
