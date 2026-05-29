import json
import logging
import sys

from tqdm import tqdm

from metadata_converter import get_schema
from metadata_converter.api_fetching.fetch import fetch_jsonld, query_source
from metadata_converter.config import ApiFetchingConfig
from metadata_converter.load import load_to_jsonld
from metadata_converter.log_setup import _log_validation_error

logger = logging.getLogger(__name__)


def fetch_api_data(config: ApiFetchingConfig) -> None:
    fetched_path = config.output.fetched
    fetched_path.mkdir(parents=True, exist_ok=True)

    logger.info("Querying %s ...", config.extractor.api_url)
    records = query_source(config.extractor)
    logger.info("Found %d record(s), fetching JSON-LD to %s", len(records), fetched_path)

    for record in tqdm(records, desc="Fetching records", unit="rec", file=sys.stdout):
        logger.debug("Fetching %s", record.doi)
        jsonld = fetch_jsonld(record, config.extractor)
        fetched_file = fetched_path / f"{record.source_id}.jsonld"
        logger.debug("Writing fetched JSON-LD to %s", fetched_file)
        write_json(jsonld, fetched_file)

    logger.info("Fetching complete. Output: %s", fetched_path)


def ingest_api_data(config: ApiFetchingConfig) -> None:
    fetched_path = config.output.fetched
    raw_files = list(fetched_path.glob("*.jsonld"))
    logger.info("Found %d fetched record(s) in %s", len(raw_files), fetched_path)

    config.output.ingested.mkdir(parents=True, exist_ok=True)

    failures = 0
    for raw_file in tqdm(raw_files, desc="Ingesting records", unit="rec", file=sys.stdout):
        try:
            with raw_file.open() as f:
                jsonld = json.load(f)
            schema_type = jsonld["@type"].split("/")[-1]
            schema = get_schema(schema_type)(**jsonld)
            load_to_jsonld(schema, output_path=config.output.ingested)
        except Exception as e:
            logger.error("Failed to ingest %s", raw_file.name)
            _log_validation_error(e, logger)
            failures += 1

    if failures:
        raise RuntimeError(
            f"{failures} of {len(raw_files)} record(s) failed to ingest — "
            "check the log for details"
        )
    logger.info("Ingestion complete. Output: %s", config.output.ingested)
