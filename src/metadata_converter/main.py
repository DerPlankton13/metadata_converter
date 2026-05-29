import logging

from metadata_converter.api_fetching.run import fetch_api_data, ingest_api_data
from metadata_converter.biosamples.run import fetch_raw_biosamples, ingest_biosamples, uplift_biosamples
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
    phase, config, logging_level = parse_cli()
    setup_logging(logging_level)

    if phase == "fetch":
        if isinstance(config, BiosamplesConfig):
            fetch_raw_biosamples(config)
        elif isinstance(config, ApiFetchingConfig):
            fetch_api_data(config)
        else:
            logger.error("fetch phase is not supported for workflow_type '%s'", config.workflow_type)
            raise SystemExit(1)

    elif phase == "ingest":
        if isinstance(config, FlatDataConfig):
            generate_jsonld(config)
        elif isinstance(config, BiosamplesConfig):
            ingest_biosamples(config)
        elif isinstance(config, ApiFetchingConfig):
            ingest_api_data(config)
        else:
            logger.error("ingest phase is not supported for workflow_type '%s'", config.workflow_type)
            raise SystemExit(1)

    elif phase == "uplift":
        if isinstance(config, UpliftingConfig):
            if config.biosamples:
                uplift_biosamples(config.biosamples)
            else:
                logger.warning("Uplift config has no sources configured — nothing to do")
        else:
            logger.error("uplift phase requires an uplifting config (workflow_type='uplifting')")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
