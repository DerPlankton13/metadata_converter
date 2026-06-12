import logging
from pathlib import Path

import pandas as pd

from metadata_converter.config import (
    CrossSheetRef,
    FlatDataConfig,
    FlatDataUpliftConfig,
)
from metadata_converter.flat_data.extract import extract_data
from metadata_converter.flat_data.schema_builder import extract_schemas
from metadata_converter.flat_data.transform import (
    add_combined_columns,
    add_id,
    clean_dataframe,
    convert_to_long,
)
from metadata_converter.flat_data.transform_helpers import split_field
from metadata_converter.flat_data.uplifting import LinkEngine, to_lookup_key
from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)

# (rule, ref_type, collected @id strings)
CollectedRef = tuple[CrossSheetRef, str, list[str]]


def ingest_flat_data(config: FlatDataConfig) -> None:
    """A wrapper to handle dict and single file input paths"""
    logger.info("Starting flat-data workflow")
    file_path = config.extractor.file_path
    if file_path.is_dir():
        files = sorted(file_path.glob("*.xlsx")) + sorted(file_path.glob("*.xls"))
        if not files:
            logger.error("No Excel files found in %s", file_path)
            raise SystemExit(1)
        logger.info("Found %d file(s) in %s", len(files), file_path)
        for excel_file in files:
            ingest_one(config, excel_file)
    else:
        ingest_one(config, file_path)


def ingest_one(config: FlatDataConfig, file_path: Path) -> None:
    """Run the full ingest pipeline for a single input file."""
    logger.info("Ingesting %s", file_path.name)
    data_dict = extract_data(config, file_path=file_path)
    data_dict = transform_wide(data_dict, config)
    collected_refs = collect_cross_ref_ids(data_dict, config)
    data_dict = transform_long(data_dict, config)
    results = build_schemas(data_dict, config)
    results = inject_cross_refs(results, collected_refs)
    write_schemas(results, config.output.ingested)


def transform_wide(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, pd.DataFrame]:
    """Clean, combine columns, and add @id — keeps wide format for cross-ref collection."""
    new_data: dict[str, pd.DataFrame] = {}
    for name, data in data_dict.items():
        logger.info("Transforming sheet '%s'", name)
        data = clean_dataframe(data, config.cleaning)
        if combines := config.combined_columns.get(name):
            data = add_combined_columns(data, combines)
        data = add_id(data, config.mapping[name]["type"])
        new_data[name] = data
    return new_data


def collect_cross_ref_ids(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> list[CollectedRef]:
    """Collect @id lists for each cross-sheet ref rule while data is still wide-format.

    Wide format is required because filter_column and @id are still actual columns here.
    The ref_type is captured now so the injection step needs no access to the config.
    """
    collected: list[CollectedRef] = []
    for ref in config.cross_sheet_refs:
        src = data_dict[ref.from_sheet]
        if ref.filter_column is not None:
            filter_key = to_lookup_key(ref.filter_value)
            src = src[src[ref.filter_column].map(to_lookup_key) == filter_key]
        ids = src["@id"].dropna().tolist()
        ref_type = config.mapping[ref.from_sheet]["type"]
        collected.append((ref, ref_type, ids))
    return collected


def transform_long(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, pd.DataFrame]:
    """Convert wide-format DataFrames to long format and split multi-value fields."""
    new_data: dict[str, pd.DataFrame] = {}
    for name, data in data_dict.items():
        data = convert_to_long(data)
        for field in config.split_fields.get(name, []):
            data = split_field(data, field)
        new_data[name] = data
    return new_data


def build_schemas(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, list[SchemaOrgBase]]:
    """Build schema.org objects from each sheet's long-format DataFrame."""
    results = {}
    for name, data in data_dict.items():
        logger.info("Building schemas for sheet '%s'", name)
        results[name] = extract_schemas(data, config.mapping[name])
    return results


def inject_cross_refs(
    results: dict[str, list[SchemaOrgBase]],
    collected: list[CollectedRef],
) -> dict[str, list[SchemaOrgBase]]:
    """Inject pre-collected cross-sheet references into already-built schemas."""
    for ref, ref_type, ids in collected:
        if not ids:
            logger.warning(
                "cross_sheet_refs: no sources for %s.%s", ref.on_sheet, ref.property
            )
            continue
        ref_cls = get_schema(ref_type)
        refs = [ref_cls(id=i) for i in ids]
        value = refs if len(refs) > 1 else refs[0]
        for schema in results.get(ref.on_sheet, []):
            setattr(schema, ref.property, value)
    return results


def write_schemas(results: dict[str, list[SchemaOrgBase]], output_path: Path) -> None:
    """Flatten all built schemas and write each to its own JSON-LD file."""
    schemas = [s for schemas in results.values() for s in schemas]
    logger.info("Writing %d JSON-LD file(s) to %s", len(schemas), output_path)
    for schema in schemas:
        load_to_jsonld(schema, output_path=output_path)


def uplift_flat_data(config: FlatDataUpliftConfig) -> None:
    """Resolve cross-references in ingested flat_data JSON-LD."""
    LinkEngine(config).run()
