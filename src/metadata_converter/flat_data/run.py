import logging

from metadata_converter.config import FlatDataConfig, SourceConfig
from metadata_converter.extract import extract_data
from metadata_converter.flat_data.transform import (
    add_id,
    clean_dataframe,
    combine_columns,
    convert_to_long,
    extract_schemas,
)
from metadata_converter.flat_data.transform_helpers import split_field
from metadata_converter.load import load_to_jsonld

logger = logging.getLogger(__name__)


def ingest_flat_data(config: FlatDataConfig) -> None:
    logger.info("Starting flat-data workflow")

    # Extract Step
    logger.info("Extracting data from %s", config.extractor.file_path)
    data_dict = extract_data(config)

    # Transform Step
    for name, data in data_dict.items():
        logger.info("Cleaning sheet '%s'", name)
        sheet_mapping = config.mapping[name]
        data = clean_dataframe(data, config.cleaning)
        combine_columns(data, sheet_mapping)
        data = add_id(data, sheet_mapping["type"])
        data = convert_to_long(data)
        for field in config.split_fields.get(name, []):
            data = split_field(data, field)
        data_dict[name] = data

    # Build schemas
    results = {}
    for name, data in data_dict.items():
        logger.info("Building schemas for sheet '%s'", name)
        results[name] = extract_schemas(data, config.mapping[name])

    # Load Step
    schemas = [s for schemas in results.values() for s in schemas]
    logger.info(
        "Writing %d JSON-LD file(s) to %s", len(schemas), config.output.ingested
    )
    for schema in schemas:
        load_to_jsonld(schema, output_path=config.output.ingested)


def uplift_flat_data(config: SourceConfig) -> None:
    """Resolve cross-references in ingested flat_data JSON-LD."""
    from metadata_converter.flat_data.uplifting import DatahubLinker

    DatahubLinker(config).run()
