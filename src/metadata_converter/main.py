import tomllib
from typing import Any

import pandas as pd
import pydantic
import schema_org_models
from nanoid import generate
from pydantic import ValidationError

from src.metadata_converter.schema_org_models import SchemaDotOrgBase


def main():

    data = pd.read_csv("dummy.csv", skipinitialspace=True).to_dict("index")
    with open("mapping.toml", "rb") as file:
        header_mapping = tomllib.load(file)

    # go through the data column by column
    for column in data.values():
        schemas = extract_schemas(column, header_mapping)
        print(schemas)


def extract_schemas(
    column: dict[str, Any], header_mapping: dict[str, Any]
) -> dict[str, SchemaDotOrgBase]:

    schemas: dict[str, schema_org_models.SchemaDotOrgBase] = {}
    # do the loop several times to do nested linkings
    for i in range(10):
        # go through all mappings defined in the toml
        for schema_type, mapping in header_mapping.items():
            # schema was already created
            if schema_type in schemas.keys():
                continue
            schema_properties = extract_properties(
                column, mapping, schema_type, schemas
            )
            # required linking not yet done, go back to the loop
            if schema_properties is None:
                continue
            try:
                schemas[schema_type] = getattr(schema_org_models, schema_type)(
                    **schema_properties
                )
            except ValidationError as e:
                print(
                    f"Could not create a class of {schema_type}: ",
                    e.errors()[0]["msg"],
                    e.errors()[0]["loc"],
                )
                continue
    return schemas


def extract_properties(
    column: dict[str, Any], mapping, schema_type, schemas: dict[str, SchemaDotOrgBase]
) -> dict[Any, Any] | None:
    schema_properties = {}
    for property, header in mapping.items():
        # does the linking
        if "link:" in header:
            type = header.split(":")[1]
            if type in schemas.keys():
                schema_properties[property] = f'{{"@id": "{schemas[type].id}.json"}}'
            else:
                return None
        else:
            schema_properties[property] = column[header]

        # ensures that there is always an id
        if "id" not in schema_properties.keys():
            # todo: think about other ways to generate the id
            unique_id = generate()
            schema_properties["id"] = schema_type + f"_{unique_id}"
    return schema_properties


if __name__ == "__main__":
    main()
