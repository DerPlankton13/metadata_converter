from typing import Any

import pandas as pd
from nanoid import generate
from pydantic import ValidationError

from metadata_converter.schema_org_registry import SchemaOrgBase, SchemaRegistry


def remove_newlines(df: pd.DataFrame) -> pd.DataFrame:
    """removes the newline characters from a pandas dataframe header and values"""
    df.columns = df.columns.str.replace("\n", "")
    df = df.replace(r"\n", "", regex=True)
    return df


def remove_whitespaces(df: pd.DataFrame) -> pd.DataFrame:
    # strip leading/trailing whitespace from all column names
    df.columns = df.columns.str.strip()
    return df


def combine_columns(df: pd.DataFrame, mapping: dict[str, Any]) -> pd.DataFrame:
    """Adds new combined columns to the dataframe

    Parameters
    ----------
    df
    mapping

    Returns
    -------

    """
    for model, props in mapping.items():
        if type(props) is dict:
            for key, value in props.items():
                if "+" in value:
                    # adds the values from the columns
                    columns = [col.strip() for col in value.split("+")]
                    df[key] = df[columns].agg(" ".join, axis=1)
                    mapping[model][key] = key

    return df


def extract_schemas(df: pd.DataFrame, mapping: dict[str, Any]) -> list[SchemaOrgBase]:

    registry = SchemaRegistry()
    schemas = []
    for _, row in df.iterrows():
        # go through all mappings defined in the toml
        for schema_type, properties in mapping.items():
            schema_properties = {}
            for prop, header in properties.items():
                schema_properties[prop] = row[header]
            # ensures that there is always an id
            if "id" not in schema_properties.keys() or schema_properties["id"] is pd.NA:
                # todo: think about other ways to generate the id
                unique_id = generate()
                schema_properties["id"] = schema_type + f"_{unique_id}"
            try:
                schemas.append(registry.get(schema_type)(**schema_properties))
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
