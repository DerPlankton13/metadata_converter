import logging
from pathlib import Path

from metadata_converter.flat_data.config import FlatDataConfig
from metadata_converter.flat_data.extract import extract_data
from metadata_converter.flat_data.transform.add_columns import add_columns
from metadata_converter.flat_data.transform.clean import clean
from metadata_converter.flat_data.transform.id_refs_broadcasting import (
    broadcast_id_refs,
    extract_inline_id_ref_broadcasts,
    prepare_id_ref_broadcast,
)
from metadata_converter.flat_data.transform.reshape import reshape
from metadata_converter.flat_data.transform.schema_builder import build_schemas
from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.schemaorg_models import (
    SchemaOrgBase,
    validate_strict,
)
from metadata_converter.utils.provenance_writer import write_provenance_file

logger = logging.getLogger(__name__)


def load_flat_data(config: FlatDataConfig) -> None:
    """Entry point: dispatch single file vs directory of Excel files."""
    logger.info("Starting flat-data workflow")
    extract_inline_id_ref_broadcasts(config)
    input = config.extractor.input
    if input.is_dir():
        files = sorted(input.glob("*.xlsx")) + sorted(input.glob("*.xls"))
        if not files:
            logger.error("No Excel files found in %s", input)
            raise SystemExit(1)
        logger.info("Found %d file(s) in %s", len(files), input)
        for excel_file in files:
            load_single(config, excel_file)
    else:
        load_single(config, input)


def load_single(config: FlatDataConfig, input: Path) -> None:
    """Run the load pipeline for a single input file."""
    logger.info("Loading %s", input.name)
    data = extract_data(config, input=input)
    data = clean(data, config)
    data = add_columns(data, config)
    refs = prepare_id_ref_broadcast(data, config)
    data = reshape(data, config)
    schemas = build_schemas(data, config)
    schemas = broadcast_id_refs(schemas, refs)
    if config.provenance_dir:
        for schema_list in schemas.values():
            for schema in schema_list:
                write_provenance_file(
                    schema.id, config.provenance_dir, str(input), "load"
                )
    write_schemas(schemas, config.output_dir)


def write_schemas(
    schema_dict: dict[str, list[SchemaOrgBase]], output_dir: Path
) -> None:
    """Flatten all built schemas and write each to its own JSON-LD file."""
    schemas = [s for schemas in schema_dict.values() for s in schemas]
    logger.info("Writing %d JSON-LD file(s) to %s", len(schemas), output_dir)
    for schema in schemas:
        load_to_jsonld(schema, output_dir=output_dir)
        try:
            validate_strict(schema)
        except ValueError as e:
            logger.warning("Strict validation failed for %s: %s", schema.id, e)
