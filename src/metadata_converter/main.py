import logging

from metadata_converter.api_fetching.run import fetch_api_data, load_api_data
from metadata_converter.biosamples.run import (
    fetch_biosamples,
    load_biosamples,
    uplift_biosamples,
)
from metadata_converter.config import (
    ApiFetchingConfig,
    ApiFetchingUpliftConfig,
    BiosamplesConfig,
    BiosamplesUpliftConfig,
    FlatDataConfig,
    FlatDataUpliftConfig,
)
from metadata_converter.flat_data.run import load_flat_data
from metadata_converter.flat_data.uplift import run_uplift
from metadata_converter.parse import parse_cli
from metadata_converter.utils.log_setup import setup_logging

logger = logging.getLogger(__name__)


def main():
    phase, config, logging_level = parse_cli()
    setup_logging(logging_level)

    match (phase, config):
        case ("fetch", BiosamplesConfig()):
            fetch_biosamples(config)
        case ("fetch", ApiFetchingConfig()):
            fetch_api_data(config)
        case ("load", FlatDataConfig()):
            load_flat_data(config)
        case ("load", BiosamplesConfig()):
            load_biosamples(config)
        case ("load", ApiFetchingConfig()):
            load_api_data(config)
        case ("uplift", BiosamplesUpliftConfig()):
            uplift_biosamples(config)
        case ("uplift", FlatDataUpliftConfig()):
            run_uplift(config)
        case ("uplift", ApiFetchingUpliftConfig()):
            logger.error("Phase 'uplift' is not implemented yet for source_type 'api_fetching'")
            raise SystemExit(1)
        case _:
            source = getattr(config, "source_type", "uplift")
            logger.error(
                "Phase '%s' is not supported for source_type '%s'",
                phase,
                source,
            )
            raise SystemExit(1)


if __name__ == "__main__":
    main()
