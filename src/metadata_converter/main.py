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
    parser = argparse.ArgumentParser(description="Metadata Converter")
    parser.add_argument("config", type=Path, help="Path to TOML config file")
    args = parser.parse_args()

    try:
        with open(args.config, "rb") as f:
            config = Config(**tomllib.load(f))
    except FileNotFoundError:
        print(f"Error: config file not found: {args.config}")
        raise SystemExit(1)
    except ValidationError as e:
        print(f"Error: invalid config:\n{e}")
        print(
            e.errors()[0]["msg"],
            e.errors()[0]["loc"],
            "but input was:",
            e.errors()[0]["input"],
        )
        raise SystemExit(1)

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
