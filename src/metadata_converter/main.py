import argparse
import tomllib
from pathlib import Path

from pydantic import ValidationError

from metadata_converter.config import Config
from metadata_converter.transform import (
    combine_columns,
    extract_schemas,
    remove_newlines,
    replace_nan_by_none,
)
from src.metadata_converter.extract import load_data
from src.metadata_converter.load import load_to_jsonld


def main():
    config = parse_cli()

    # Extract Step
    data = load_data(config)

    # Transform Step
    data = remove_newlines(data)
    data = combine_columns(data, config.mapping)
    data = replace_nan_by_none(data)
    schema_list = extract_schemas(data, config)

    # Load Step
    for schema in schema_list:
        print(schema)
        load_to_jsonld(schema, output_path=Path("output"))


if __name__ == "__main__":
    main()
