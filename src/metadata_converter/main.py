import logging

from metadata_converter.api_fetching.run import fetch_api_data, ingest_api_data
from metadata_converter.biosamples.run import (
    fetch_biosamples,
    ingest_biosamples,
    uplift_biosamples,
)
from metadata_converter.config import (
    ApiFetchingConfig,
    BiosamplesConfig,
    FlatDataConfig,
    UpliftingConfig,
)
from metadata_converter.flat_data.run import flat_data_etl
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
        case ("ingest", FlatDataConfig()):
            flat_data_etl(config)
        case ("ingest", BiosamplesConfig()):
            ingest_biosamples(config)
        case ("ingest", ApiFetchingConfig()):
            ingest_api_data(config)
        case ("uplift", UpliftingConfig()):
            if config.biosamples:
                uplift_biosamples(config.biosamples)
            if config.flat_data:
                run_uplift(config.flat_data)
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
