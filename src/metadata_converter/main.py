from pathlib import Path

import pandas as pd

from metadata_converter.extract import extract_data
from metadata_converter.load import load_to_jsonld
from metadata_converter.parse import parse_cli
from metadata_converter.schema_org_models.schemaorg_models import Person
from metadata_converter.transform import (
    add_id,
    clean_dataframe,
    convert_to_long,
    extract_schemas,
)

SPLIT_AUTHORS = False


def main():
    config = parse_cli()

    # Extract Step
    data_dict = extract_data(config)

    # Transform Step
    results = {}
    for sheet, data in data_dict.items():
        data = clean_dataframe(data, config.input.cleaning)
        data = add_id(data, schema_type)
        data = convert_to_long(data)

        # split the authors
        if SPLIT_AUTHORS:
            fields_to_split = ["Authors"]
            for header in fields_to_split:
                field_df = data[data.header == header].copy()
                non_field_df = data[data.header != header]

                field_df.value = field_df.value.str.split(r"\s*[,;&]\s*|\s+and\s+")
                exploded_df = field_df.explode("value")
                data = pd.concat([non_field_df, exploded_df], ignore_index=True)

        if config.sheet_type_mapping:
            schema_type = config.sheet_type_mapping[sheet]

        else:
            if len(config.mapping) > 1:
                raise ConfigError(
                    "The config.mapping is invalid. If 'sheet_type_mapping' is not "
                    "defined, only one mapping is allowed."
                )
            schema_type = list(config.mapping)[0]

        results[schema_type] = extract_schemas(
            data, schema_type, config.mapping[schema_type]
        )

    # some postprocessing
    for schema_type, schemas in results.items():
        if schema_type == "Person":
            for schema in schemas:
                if schema.name is None:
                    schema.name = schema.givenName + schema.familyName
        elif schema_type == "Dataset":
            for schema in schemas:
                if schema.creator is None:
                    schema.creator = [Person(id=p.id) for p in results["Person"]]

    # Load Step
    for schema in [s for schemas in results.values() for s in schemas]:
        load_to_jsonld(schema, output_path=Path("output"))


if __name__ == "__main__":
    main()
