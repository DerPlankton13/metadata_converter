import argparse
from pathlib import Path

from metadata_converter.config import Config, load_config


def parse_cli() -> Config:
    parser = argparse.ArgumentParser(description="Metadata Converter")
    parser.add_argument("config", type=Path, help="Path to TOML config file")
    args = parser.parse_args()

    return load_config(args.config)
