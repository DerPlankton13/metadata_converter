import argparse
import tomllib
from pathlib import Path

from pydantic import ValidationError

from metadata_converter.config import Config


def parse_cli() -> Config:
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
    return config
