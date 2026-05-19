import logging

from metadata_converter.api_fetching.run import fetch_from_api
from metadata_converter.biosamples.run import fetch_raw_biosamples, uplift_biosamples
from metadata_converter.config import (
    ApiFetchingConfig,
    BiosamplesConfig,
    FlatDataConfig,
)
from metadata_converter.flat_data.run import generate_jsonld
from metadata_converter.logging import setup_logging
from metadata_converter.parse import parse_cli

logger = logging.getLogger(__name__)


def main():
    config, logging_level = parse_cli()
    setup_logging(logging_level, output_path=config.output.output_path)

    if isinstance(config, FlatDataConfig):
        generate_jsonld(config)

    elif isinstance(config, ApiFetchingConfig):
        fetch_from_api(config)

    elif isinstance(config, BiosamplesConfig):
        fetch_raw_biosamples(config)
        if config.uplifting is not None:
            uplift_biosamples(config)


if __name__ == "__main__":
    main()
