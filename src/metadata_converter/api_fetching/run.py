import json
import logging
import sys

from tqdm import tqdm

from metadata_converter import get_schema
from metadata_converter.api_fetching.config import ApiFetchingConfig
from metadata_converter.api_fetching.fetch import fetch_jsonld, query_source
from metadata_converter.api_fetching.fixers import FIXERS
from metadata_converter.load import load_to_jsonld
from metadata_converter.utils.hashing import hashed_id
from metadata_converter.utils.io import write_json
from metadata_converter.utils.log_setup import log_validation_error
from metadata_converter.utils.provenance_writer import write_provenance_file

logger = logging.getLogger(__name__)


def fetch_api_data(config: ApiFetchingConfig) -> None:
    logger.info("Starting API fetch from %s", config.fetcher.api_url)

    fetched_path = config.fetched_dir
    fetched_path.mkdir(parents=True, exist_ok=True)

    records = query_source(config.fetcher)
    logger.info(
        "Found %d record(s), fetching JSON-LD to %s", len(records), fetched_path
    )

    for record in tqdm(records, desc="Fetching records", unit="rec", file=sys.stdout):
        logger.debug("Fetching %s", record.doi)
        jsonld = fetch_jsonld(record, config.fetcher)
        fetched_file = fetched_path / f"{record.source_id}.jsonld"
        logger.debug("Writing fetched JSON-LD to %s", fetched_file)
        write_json(jsonld, fetched_file)

        if config.provenance_dir is not None:
            if config.fetcher.fetch_strategy == "export_endpoint":
                source_url = config.fetcher.export_url_template.format(
                    record_id=record.source_id
                )
            else:
                source_url = record.url
            record_id = jsonld.get("@id") or hashed_id(jsonld)
            write_provenance_file(record_id, config.provenance_dir, source_url, "load")

    logger.info("API fetch complete. Output: %s", fetched_path)


def load_api_data(config: ApiFetchingConfig) -> None:
    logger.info("Starting API load")

    fetched_path = config.fetched_dir
    fetched_files = list(fetched_path.glob("*.jsonld"))
    logger.info("Found %d fetched record(s) in %s", len(fetched_files), fetched_path)

    config.output_dir.mkdir(parents=True, exist_ok=True)

    failures = 0
    for fetched_file in tqdm(
        fetched_files, desc="Loading records", unit="rec", file=sys.stdout
    ):
        try:
            with fetched_file.open() as f:
                jsonld = json.load(f)
            for fixer_name in config.fixers:
                jsonld = FIXERS[fixer_name](jsonld)
            schema_type = jsonld["@type"].split("/")[-1]
            schema = get_schema(schema_type)(**jsonld)
            load_to_jsonld(schema, output_dir=config.output_dir)
        except Exception as e:
            logger.error("Failed to load %s", fetched_file.name)
            log_validation_error(e, logger)
            failures += 1

    if failures:
        raise RuntimeError(
            f"{failures} of {len(fetched_files)} record(s) failed to load — "
            "check the log for details"
        )
    logger.info("API load complete. Output: %s", config.output_dir)
