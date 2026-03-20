from typing import Any

import pandas as pd
from nanoid import generate
from pydantic import ValidationError

from metadata_converter.config import CleaningConfig, Config
from metadata_converter.schema_org_models.schemaorg_models import (
    SchemaOrgBase,
    get_schema,
)


def clean_dataframe(df: pd.DataFrame, config: CleaningConfig) -> pd.DataFrame:

    if config.strip_header_whitespace:
        df.columns = df.columns.str.replace(r"[\s\n]+", " ", regex=True).str.strip()

    if config.strip_cell_whitespace:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(
            lambda col: col.str.replace(r"[\s\n]+", " ", regex=True).str.strip()
        )

    if config.normalize_empty_to_nan:
        df.replace({s: None for s in config.empty_sentinels}, inplace=True)

    if config.placeholders_to_nan:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(
            lambda col: col.where(
                ~col.str.match(config.placeholder_pattern, na=False), other=pd.NA
            )
        )

    df = df.convert_dtypes()  # after cleaning, infer best types

    if config.drop_fully_empty_rows:
        df.dropna(how="all", inplace=True)

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
