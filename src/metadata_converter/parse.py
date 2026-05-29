import argparse
import logging
from pathlib import Path

from metadata_converter.config import Config, load_config


def parse_cli() -> tuple[str, Config, int]:
    parser = argparse.ArgumentParser(description="Metadata Converter")
    parser.add_argument(
        "phase",
        choices=["fetch", "ingest", "uplift"],
        help="Pipeline phase to execute",
    )
    parser.add_argument("config", type=Path, help="Path to TOML config file")
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default="info",
        help="Set logging verbosity (default: info)",
    )
    args = parser.parse_args()
    logging_level = getattr(logging, args.log_level.upper())
    return args.phase, load_config(args.config), logging_level
