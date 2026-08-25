import argparse
import logging
from pathlib import Path

from metadata_converter.config import (
    ApiFetchingConfig,
    BiosamplesConfig,
    BiosamplesUpliftRecordConfig,
    FlatDataConfig,
    GenericUpliftConfig,
    load_source_config,
    load_uplift_config,
    load_uplift_record_config,
)

UpliftConfig = BiosamplesUpliftRecordConfig | GenericUpliftConfig


def parse_cli() -> tuple[str, FlatDataConfig | BiosamplesConfig | ApiFetchingConfig | UpliftConfig, int]:
    parser = argparse.ArgumentParser(description="Metadata Converter")
    parser.add_argument(
        "phase",
        choices=["fetch", "load", "uplift_record", "uplift"],
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

    if args.phase == "uplift":
        config = load_uplift_config(args.config)
    elif args.phase == "uplift_record":
        config = load_uplift_record_config(args.config)
    else:
        config = load_source_config(args.config)

    return args.phase, config, logging_level