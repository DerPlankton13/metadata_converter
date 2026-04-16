import re
from typing import Any

import pandas as pd
from nanoid import generate
from pydantic import ValidationError

from metadata_converter.cleaning_plugin import CleaningPlugin
from metadata_converter.config import CleaningConfig
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import (
    SchemaOrgBase,
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


def convert_to_long(df: pd.DataFrame, sheet_name: str = None) -> pd.DataFrame:
    """Converts the data into a long format"""
    df["id"] = df.index.astype(str)
    if sheet_name:
        df.id = sheet_name + "_" + df.id
    return df.melt(id_vars=["id"], var_name="header")


def add_id(data: pd.DataFrame, schema_type: str) -> pd.DataFrame:
    """Generate a unique identifier for a schema instance."""
    data["@id"] = [f"{schema_type}_{generate()}.jsonld" for _ in range(len(data))]
    return data


def instantiate_schema(
    schema_type: str, schema_properties: dict
) -> SchemaOrgBase | None:
    """
    Instantiate a schema.org object from a type and pre-resolved properties.

    Parameters
    ----------
    schema_type : str
        The schema.org type to instantiate (e.g. ``"Person"``, ``"Event"``).
    schema_properties : dict
        A dictionary of already-resolved schema properties, ready to be passed
        to the ``SchemaOrgBase`` constructor.

    Returns
    -------
    SchemaOrgBase or None
        Instantiated schema object, or ``None`` if validation failed.
    """
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


def build_schema(
    entity: dict[str, Any], mapping: dict, nested: bool = False
) -> list[SchemaOrgBase]:
    """
    Orchestrate extraction and instantiation of one or more schema.org objects.

    If the entity contains parallel lists of equal length, one instance is
    built per row. Otherwise, a single instance is built. Delegates property
    extraction to :func:`extract_properties` and instantiation to
    :func:`instantiate_schema`.

    Parameters
    ----------
    entity : dict[str, Any]
        A dictionary of field names to their values, representing one record.
    mapping : dict
        Mapping of schema properties to data fields. Values can be either:
        - str : a field name in ``entity``
        - dict : a nested schema definition (must include a ``"type"`` key)
        - list : a list of nested schema definitions (each must include a ``"type"`` key)
    nested : bool, optional
        If ``True``, suppresses auto-generation of a top-level ``id``.
        Used when building nested (child) schema objects. Default is ``False``.

    Returns
    -------
    list of SchemaOrgBase
        A list of instantiated schema objects. Empty if validation failed for
        all instances.

    Raises
    ------
    KeyError
        If a referenced field name does not exist in ``entity``.
    ValueError
        If a nested mapping dict is missing a ``"type"`` key.
    """
    schema_type = mapping.get("type")
    if not schema_type:
        raise ValueError(
            f"Missing 'type' in {'nested ' if nested else ''}schema for '{mapping}'"
        )

    mapping = {k: v for k, v in mapping.items() if k != "type"}
    schema_properties = extract_properties(entity, mapping)

    if len(schema_properties) == 0:
        return []

    if nested and is_multi_instance(schema_properties):
        schema_properties = split_properties(schema_properties)
    else:
        schema_properties = [schema_properties]

    schemas = []
    for schema_prop in schema_properties:
        schema = instantiate_schema(schema_type, schema_prop)
        if schema is not None:
            schemas.append(schema)

    return schemas


def extract_properties(entity: dict[str, Any], mapping: dict) -> dict[Any, Any]:
    """
    Extract and resolve schema properties from an entity using a mapping definition.

    Iterates over the mapping and resolves each entry as a simple field lookup,
    a nested schema object, or a list of nested schema objects.

    Parameters
    ----------
    entity : dict[str, Any]
        A dictionary of field names to their values, representing one record.
    mapping : dict
        Mapping of schema property names to field definitions. See
        :func:`build_schema` for the supported value formats.

    Returns
    -------
    dict
        A dictionary of resolved schema properties, ready to be passed to
        :func:`instantiate_schema`.

    Raises
    ------
    KeyError
        If a simple field name referenced in ``mapping`` does not exist in ``entity``.
    TypeError
        If a list mapping contains a non-dict element.
    ValueError
        If a nested mapping dict is missing a ``"type"`` key.
    """
    schema_properties = {}

    for prop, value in mapping.items():
        if isinstance(value, str):
            var = get_field_value(entity, value)
            if var is not None:
                schema_properties[prop] = var

        elif isinstance(value, dict):
            nested_schemas = build_schema(entity, value, nested=True)
            if nested_schemas:
                schema_properties[prop] = (
                    nested_schemas[0] if len(nested_schemas) == 1 else nested_schemas
                )

        elif isinstance(value, list):
            for schema_mapping in value:
                if not isinstance(schema_mapping, dict):
                    raise TypeError(
                        f"The elements of an array should be a dict. "
                        f"{schema_mapping} is not a dict."
                    )
                nested_schemas = build_schema(entity, schema_mapping, nested=True)
                if nested_schemas:
                    schema_properties.setdefault(prop, []).extend(nested_schemas)

    return schema_properties


def get_field_value(entity: dict[str, Any], value: str):
    """
    Retrieve and clean a field's value from an entity dictionary.

    Handles both scalar and list-typed values, strips ``NaN``/``None``,
    and returns a scalar if only one value remains or a list if multiple do.

    Parameters
    ----------
    entity : dict[str, Any]
        A dictionary of field names to their values, representing one record.
    value : str
        The field name to look up in ``entity``.

    Returns
    -------
    scalar, list, or None
        The field value (or list of values) with missing entries removed.
        Returns ``None`` if all values were missing.

    Raises
    ------
    KeyError
        If ``value`` is not a key in ``entity``.
    """
    if value not in entity:
        raise KeyError(f"Header '{value}' not found in the data.")
    field_values = entity[value]
    if not isinstance(field_values, list):
        field_values = [field_values]

    field_values = [v for v in field_values if pd.notna(v)]

    if not field_values:
        return None
    return field_values[0] if len(field_values) == 1 else field_values


def is_multi_instance(schema_properties: dict[str, Any]) -> bool:
    """
    Check whether an entity contains parallel lists of equal length.

    Parameters
    ----------
    schema_properties : dict[str, Any]
        A dictionary of field names to their values.

    Returns
    -------
    bool
        ``True`` if all values are lists of the same length greater than 1,
        ``False`` otherwise.
    """
    lengths = {len(v) for v in schema_properties.values() if isinstance(v, list)}
    return len(lengths) == 1 and lengths.pop() > 1


def split_properties(schema_properties: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Split a multi-value entity dict into a list of single-value dicts.

    Parameters
    ----------
    schema_properties : dict[str, Any]
        A dictionary where all values are lists of equal length.

    Returns
    -------
    list of dict
        One dictionary per row, each containing a single value per key.
    """
    keys = list(schema_properties.keys())
    values = [
        schema_properties[k]
        if isinstance(schema_properties[k], list)
        else [schema_properties[k]]
        for k in keys
    ]
    return [dict(zip(keys, row)) for row in zip(*values)]


def extract_schemas(df: pd.DataFrame, mapping: dict[str, Any]) -> list[SchemaOrgBase]:
    """
    Convert a pandas DataFrame into a list of schema.org objects.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data in long format with at least ``"id"``, ``"header"``, and
        ``"value"`` columns.
    mapping : dict
        Dict containing schema type-to-property mappings.

    Returns
    -------
    list of SchemaOrgBase
        A list of instantiated and Pydantic-validated schema.org objects.
    """
    schemas = []

    for _, entity in df.groupby("id"):
        entity = entity.groupby("header")["value"].apply(list).to_dict()
        result = build_schema(entity, mapping)
        if result is not None:
            schemas.extend(result)

    return schemas
