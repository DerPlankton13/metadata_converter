import json
import logging

from pydantic import ValidationError
from tqdm import tqdm

from metadata_converter.api_fetching.fetch import fetch_jsonld, query_source
from metadata_converter.biosamples.run import fetch_raw_biosamples
from metadata_converter.biosamples.uplifting import SampleUplifter
from metadata_converter.config import (
    BiosamplesConfig,
    FlatDataConfig,
    MetadataCollectorConfig,
)
from metadata_converter.flat_data.run import generate_jsonld
from metadata_converter.load import load_to_jsonld
from metadata_converter.logging import _log_validation_error, setup_logging
from metadata_converter.parse import parse_cli
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import (
    Action,
    Product,
    SchemaOrgBase,
)

logger = logging.getLogger(__name__)


def main():
    config, logging_level = parse_cli()
    setup_logging(logging_level, output_path=config.output.output_path)

    if isinstance(config, FlatDataConfig):
        generate_jsonld(config)

    elif isinstance(config, MetadataCollectorConfig):
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
        logger.info(
            "Writing %d schema(s) to %s", len(results), config.output.output_path
        )
        for key, schema in results.items():
            load_to_jsonld(schema, output_path=config.output.output_path)

    elif isinstance(config, BiosamplesConfig):
        fetch_raw_biosamples(config)
        if config.uplifting is not None:
            raw_files = config.output.output_path.glob("**/*.jsonld")
            for path in tqdm(list(raw_files), desc="Uplifting samples", unit="sample"):
                with path.open() as f:
                    raw = json.load(f)
                try:
                    uplifter = SampleUplifter(raw)
                    product_dict, action_dict = uplifter.build_dicts()
                except Exception as e:
                    logger.error("Failed to uplift %s: %s", path.name, e)
                    continue
                try:
                    product = Product(**product_dict)
                    load_to_jsonld(product, output_path=config.uplifting.output_path)
                except ValidationError as e:
                    logger.error("Failed to build product for %s.", path.name)
                    _log_validation_error(e, logger)
                try:
                    action = Action(**action_dict)
                    load_to_jsonld(action, output_path=config.uplifting.output_path)
                except ValidationError as e:
                    logger.error("Failed to build action for %s.", path.name)
                    _log_validation_error(e, logger)


if __name__ == "__main__":
    main()
