import logging

from metadata_converter.api_fetching.run import fetch_from_api
from metadata_converter.biosamples.run import fetch_raw_biosamples, uplift_biosamples
from metadata_converter.config import (
    ApiFetchingConfig,
    BiosamplesConfig,
    FlatDataConfig,
    UpliftingConfig,
)
from metadata_converter.flat_data.run import generate_jsonld
from metadata_converter.log_setup import setup_logging
from metadata_converter.parse import parse_cli

logger = logging.getLogger(__name__)


def main():
    config, logging_level = parse_cli()

    if isinstance(config, FlatDataConfig):
        setup_logging(logging_level, output_path=config.output.output_path)
        generate_jsonld(config)

    elif isinstance(config, ApiFetchingConfig):
        setup_logging(logging_level, output_path=config.output.output_path)
        fetch_from_api(config)

    elif isinstance(config, BiosamplesConfig):
        setup_logging(logging_level, output_path=config.output.output_path)
        fetch_raw_biosamples(config)

    elif isinstance(config, UpliftingConfig):
        setup_logging(logging_level, output_path=config.biosamples.output_path.parent)
        try:
            uplift_biosamples(config.biosamples)
        except Exception as e:
            logger.error("Failed to uplift biosamples", exc_info=e)


if __name__ == "__main__":
    main()
