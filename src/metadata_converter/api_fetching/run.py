import json
import logging

from tqdm import tqdm

from metadata_converter import get_schema
from metadata_converter.api_fetching.fetch import fetch_jsonld, query_source
from metadata_converter.config import ApiFetchingConfig
from metadata_converter.load import load_to_jsonld
from metadata_converter.logging import _log_validation_error
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)


def fetch_from_api(config: ApiFetchingConfig) -> None:
    results: dict[str, SchemaOrgBase] = {}
    raw_output_path = config.output.output_path / "raw"
    raw_output_path.mkdir(parents=True, exist_ok=True)

    # Extract Step
    logger.info("Querying %s ...", config.extractor.api_url)
    records = query_source(config.extractor)
    logger.info(
        "Found %d record(s), fetching JSON-LD to %s", len(records), raw_output_path
    )

    for record in tqdm(records, desc="Fetching records", unit="rec"):
        logger.debug("Fetching %s", record.doi)
        jsonld = fetch_jsonld(record, config.extractor)

        output_path = raw_output_path / f"{record.source_id}.jsonld"
        logger.debug("Writing raw JSON-LD to %s", output_path)
        output_path.write_text(
            json.dumps(jsonld, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

        # Transform Step
        try:
            schema_type = jsonld["@type"].split("/")[-1]
            results[record.doi] = get_schema(schema_type)(**jsonld)
        except Exception as e:
            logger.error("Failed to extract schema for DOI: %s", record.doi)
            _log_validation_error(e, logger)

    # Load Step
    logger.info("Writing %d schema(s) to %s", len(results), config.output.output_path)
    for key, schema in results.items():
        load_to_jsonld(schema, output_path=config.output.output_path)
