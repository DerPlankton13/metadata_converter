from pathlib import Path

from metadata_converter.extract import load_data
from metadata_converter.load import load_to_jsonld
from metadata_converter.parse import parse_cli
from metadata_converter.transform import (
    clean_dataframe,
    combine_columns,
    convert_to_long,
    extract_schemas,
)


def main():
    config = parse_cli()

    # Extract Step
    data = load_data(config)

    # Transform Step
    data = clean_dataframe(data, config.input.cleaning)
    data = combine_columns(data, config.mapping)

    # add funding and link to project
    data["Grant ID"] = (
        "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/grant_b5d.jsonld"
    )
    data["Project ID"] = (
        "https://github.com/DerPlankton13/B5D/blob/main/GeneralSchemas/project_b5d.jsonld"
    )

    data = convert_to_long(data)
    schema_list = extract_schemas(data, config)

    # Load Step
    for schema in schema_list:
        load_to_jsonld(schema, output_path=Path("output"))


if __name__ == "__main__":
    main()
