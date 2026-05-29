import logging

from metadata_converter.config import FlatDataConfig
from metadata_converter.extract import extract_data
from metadata_converter.flat_data.preprocess_datahub import preprocess_datahub
from metadata_converter.flat_data.transform import (
    add_id,
    clean_dataframe,
    convert_to_long,
    extract_schemas,
)
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
        "Writing %d JSON-LD file(s) to %s", len(schemas), config.output.ingested
    )
    for schema in schemas:
        load_to_jsonld(schema, output_path=config.output.ingested)
