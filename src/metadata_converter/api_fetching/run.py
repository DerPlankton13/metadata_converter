import json
import logging
import sys
from typing import Any

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


def standardise_id(jsonld: dict) -> dict:
    """Replace `@id` with a content hash, preserving the original as `identifier`.

    A fetched record's `@id` is always source-native (a DOI, a URL, ...); it can
    never already be one of our own content hashes, so this always rehashes —
    there is no "already standardised" case to detect here.

    Mutates and returns `jsonld` in place.
    """
    current_id = jsonld.get("@id")
    jsonld["@id"] = hashed_id({k: v for k, v in jsonld.items() if k != "@id"})
    if current_id is not None and not (
        in_property(jsonld.get("identifier"), current_id)
        or in_property(jsonld.get("url"), current_id)
    ):
        jsonld["identifier"] = current_id
    return jsonld


def in_property(prop: Any, value: Any) -> bool:
    """Check whether `value` equals `prop` or is contained in it when `prop` is a list."""
    if prop is None:
        return False
    if not isinstance(prop, list):
        prop = [prop]
    if value in prop:
        return True
    return False


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
            write_provenance_file(
                fetched_file.name, config.provenance_dir, source_url, "fetch"
            )

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
            # Standardise now so schema.id below reflects the real final id for provenance.
            jsonld = standardise_id(jsonld)
            schema = get_schema(schema_type)(**jsonld)
            load_to_jsonld(schema, output_dir=config.output_dir)
            if config.provenance_dir is not None:
                write_provenance_file(
                    schema.id, config.provenance_dir, fetched_file.name, "load"
                )
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
