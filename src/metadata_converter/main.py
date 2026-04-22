from pathlib import Path

from metadata_converter.extract import extract_data
from metadata_converter.load import load_to_jsonld
from metadata_converter.parse import parse_cli
from metadata_converter.preprocess_datahub import preprocess_datahub
from metadata_converter.transform import (
    add_id,
    clean_dataframe,
    convert_to_long,
    extract_schemas,
)


def main():
    config = parse_cli()

    # Extract Step
    data_dict = extract_data(config)

    # Transform Step
    for name, data in data_dict.items():
        data = clean_dataframe(data, config.input.cleaning)
        data = add_id(data, config.mapping[name]["type"])
        data = convert_to_long(data)
        data_dict[name] = data

    data_dict = preprocess_datahub(data_dict)

    # create the schemata
    results = {}
    for name, data in data_dict.items():
        results[name] = extract_schemas(data, config.mapping[name])

    # Load Step
    for schema in [s for schemas in results.values() for s in schemas]:
        load_to_jsonld(schema, output_path=Path("output"))


if __name__ == "__main__":
    main()
