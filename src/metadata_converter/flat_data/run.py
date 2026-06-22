import logging
from pathlib import Path

from metadata_converter.config import FlatDataConfig
from metadata_converter.flat_data.extract import extract_data
from metadata_converter.flat_data.transform.add_columns import add_columns
from metadata_converter.flat_data.transform.clean import clean
from metadata_converter.flat_data.transform.cross_sheet_refs import (
    collect_cross_ref_ids,
    inject_cross_refs,
)
from metadata_converter.flat_data.transform.reshape import reshape
from metadata_converter.flat_data.transform.schema_builder import build_schemas
from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.provenance_writer import write_provenance_file

logger = logging.getLogger(__name__)


def flat_data_etl(config: FlatDataConfig) -> None:
    """Entry point: dispatch single file vs directory of Excel files."""
    logger.info("Starting flat-data workflow")
    input = config.extractor.input
    if input.is_dir():
        files = sorted(input.glob("*.xlsx")) + sorted(input.glob("*.xls"))
        if not files:
            logger.error("No Excel files found in %s", input)
            raise SystemExit(1)
        logger.info("Found %d file(s) in %s", len(files), input)
        for excel_file in files:
            single_etl(config, excel_file)
    else:
        single_etl(config, input)


def single_etl(config: FlatDataConfig, input: Path) -> None:
    """Run the ingest pipeline for a single input file."""
    logger.info("Ingesting %s", input.name)
    data = extract_data(config, input=input)
    data = clean(data, config)
    data = add_columns(data, config)
    refs = collect_cross_ref_ids(data, config)
    data = reshape(data, config)
    schemas = build_schemas(data, config)
    schemas = inject_cross_refs(schemas, refs)
    if config.provenance_dir:
        for schema_list in schemas.values():
            for schema in schema_list:
                write_provenance_file(
                    schema.id, config.provenance_dir, str(input)
                )
    write_schemas(schemas, config.output.loaded_base)


def write_schemas(
    schema_dict: dict[str, list[SchemaOrgBase]], output_dir: Path
) -> None:
    """Flatten all built schemas and write each to its own JSON-LD file."""
    schemas = [s for schemas in schema_dict.values() for s in schemas]
    logger.info("Writing %d JSON-LD file(s) to %s", len(schemas), output_dir)
    for schema in schemas:
        load_to_jsonld(schema, output_dir=output_dir)
