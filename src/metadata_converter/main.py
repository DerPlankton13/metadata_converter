import json
import logging

from tqdm import tqdm

from metadata_converter.biosamples.run import run_biosamples_extraction
from metadata_converter.config import (
    BiosamplesConfig,
    FlatDataConfig,
    MetadataCollectorConfig,
)
from metadata_converter.extract import extract_data
from metadata_converter.flat_data.preprocess_datahub import preprocess_datahub
from metadata_converter.flat_data.transform import (
    add_id,
    clean_dataframe,
    convert_to_long,
    extract_schemas,
)
from metadata_converter.linked_data.metadata_collector import fetch_jsonld, query_source
from metadata_converter.load import load_to_jsonld
from metadata_converter.logging_setup import setup_logging
from metadata_converter.parse import parse_cli
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)


def _log_validation_error(error) -> None:
    from pydantic import ValidationError

    if not isinstance(error, ValidationError):
        return
    error_list = [{**e, "depth": len(e["loc"])} for e in error.errors()]
    max_depth = max(e["depth"] for e in error_list)
    deepest = [e for e in error_list if e["depth"] == max_depth]
    logger.error(
        "Validation error in '%s' for property '%s'",
        deepest[0]["loc"][0],
        deepest[0]["loc"][-2],
    )
    for e in deepest:
        logger.error("  %s", e["msg"])
    logger.error("Input was: %s", deepest[0]["input"])


def main():
    config, logging_level = parse_cli()
    setup_logging(logging_level, output_path=config.output.output_path)

    if isinstance(config, FlatDataConfig):
        logger.info("Starting flat-data workflow")

        # Extract Step
        logger.info("Extracting data from %s", config.extractor.file_path)
        data_dict = extract_data(config)

        # Transform Step
        for name, data in data_dict.items():
            logger.info("Cleaning sheet '%s'", name)
            data = clean_dataframe(data, config.cleaning)
            data = add_id(data, config.mapping[name]["type"])
            data = convert_to_long(data)
            data_dict[name] = data

        data_dict = preprocess_datahub(data_dict)

        # Build schemas
        results = {}
        for name, data in data_dict.items():
            logger.info("Building schemas for sheet '%s'", name)
            results[name] = extract_schemas(data, config.mapping[name])

        # Load Step
        schemas = [s for schemas in results.values() for s in schemas]
        logger.info(
            "Writing %d JSON-LD file(s) to %s", len(schemas), config.output.output_path
        )
        for schema in schemas:
            load_to_jsonld(schema, output_path=config.output.output_path)

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
                _log_validation_error(e)

        # Load Step
        logger.info(
            "Writing %d schema(s) to %s", len(results), config.output.output_path
        )
        for key, schema in results.items():
            load_to_jsonld(schema, output_path=config.output.output_path)

    elif isinstance(config, BiosamplesConfig):
        run_biosamples_extraction(config)


if __name__ == "__main__":
    main()
