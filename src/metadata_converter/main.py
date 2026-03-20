from pathlib import Path

from metadata_converter.additional_cleaners import combine_month_year_rows
from metadata_converter.parse import parse_cli
from metadata_converter.transform import (
    clean_dataframe,
    combine_columns,
    extract_schemas,
)
from src.metadata_converter.extract import load_data
from src.metadata_converter.load import load_to_jsonld


def main():
    config = parse_cli()

    # Extract Step
    data = load_data(config)

    # Transform Step
    data = combine_month_year_rows(data, "Month/ Year of publication")
    data = clean_dataframe(data, config.input.cleaning)
    data = combine_columns(data, config.mapping)
    schema_list = extract_schemas(data, config)

    # Load Step
    for schema in schema_list:
        print(schema)
        load_to_jsonld(schema, output_path=Path("output"))


if __name__ == "__main__":
    main()
