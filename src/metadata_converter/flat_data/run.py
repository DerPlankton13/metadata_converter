import itertools
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

logger = logging.getLogger(__name__)


def flat_data_etl(config: FlatDataConfig) -> None:
    """Entry point: dispatch single file vs directory of Excel files."""
    logger.info("Starting flat-data workflow")
    file_path = config.extractor.file_path
    if file_path.is_dir():
        files = sorted(file_path.glob("*.xlsx")) + sorted(file_path.glob("*.xls"))
        if not files:
            logger.error("No Excel files found in %s", file_path)
            raise SystemExit(1)
        logger.info("Found %d file(s) in %s", len(files), file_path)
        for excel_file in files:
            single_etl(config, excel_file)
    else:
        single_etl(config, file_path)


def single_etl(config: FlatDataConfig, file_path: Path) -> None:
    """Run the ingest pipeline for a single input file."""
    logger.info("Ingesting %s", file_path.name)
    data = extract_data(config, file_path=file_path)
    data = clean(data, config)
    data = add_columns(data, config)
    refs = collect_cross_ref_ids(data, config)
    data = reshape(data, config)
    schemas = build_schemas(data, config)
    schemas = inject_cross_refs(schemas, refs)
    write_schemas(schemas, config.output.ingested)


def write_schemas(
    schema_dict: dict[str, list[SchemaOrgBase]], output_path: Path
) -> None:
    """Flatten all built schemas and write each to its own JSON-LD file."""
    schemas = [s for schemas in schema_dict.values() for s in schemas]
    logger.info("Writing %d JSON-LD file(s) to %s", len(schemas), output_path)
    for schema in schemas:
        load_to_jsonld(schema, output_path=output_path)
