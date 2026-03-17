import tomllib
from pathlib import Path

from metadata_converter.transform import (
    combine_columns,
    extract_schemas,
    remove_newlines,
)
from src.metadata_converter.extract import CSVExtractor, ExcelExtractor
from src.metadata_converter.load import load_to_jsonld


def main():
    with open("mapping.toml", "rb") as file:
        mapping = tomllib.load(file)

    # Extract Step
    data = ExcelExtractor("ReportingContinuous_WP1.xlsx").execute()

    # Transform Step
    data = remove_newlines(data)
    data = combine_columns(data, mapping)
    schema_list = extract_schemas(data, mapping)

    # Load Step
    for schema in schema_list:
        print(schema)
        load_to_jsonld(schema, output_path=Path("output"))


if __name__ == "__main__":
    main()
