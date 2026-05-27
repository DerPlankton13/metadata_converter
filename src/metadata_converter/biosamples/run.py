import json
import logging
from pathlib import Path

import pandas as pd
from pydantic import ValidationError
from tqdm import tqdm

from metadata_converter.biosamples.fetch import get_metadata
from metadata_converter.biosamples.uplifting import SampleUplifter
from metadata_converter.config import BiosamplesConfig, BiosamplesInput, SourceConfig
from metadata_converter.load import load_to_jsonld
from metadata_converter.log_setup import _log_validation_error
from metadata_converter.schema_org_models.schemaorg_models import (
    Action,
    Product,
    make_strict,
)

logger = logging.getLogger(__name__)


def get_sample_ids(excel_file: Path, config: BiosamplesInput) -> set[str] | None:
    df = pd.read_excel(
        excel_file,
        sheet_name=config.sheet_name,
        header=config.header,
        skiprows=config.skiprows,
    )
    df = df.dropna(how="all")

    if config.header_name not in df.columns:
        logger.error(
            f"Column '{config.header_name}' not found in sheet '{config.sheet_name}' of '{excel_file.name}'. Skipping this file now."
        )
        return None

    return set(df[config.header_name].dropna().tolist())


def modify_context(metadata: dict, sample_id: str) -> dict:
    """Puts schema.org into context's @vocab to avoid issues with rdflib."""
    context = metadata.get("@context")
    if not context:
        logger.error("No '@context' found for sample %s", sample_id)
    else:
        try:
            terms = context[1]
            context = {"@vocab": "https://schema.org/", **terms}
            metadata["@context"] = context
        except (IndexError, TypeError):
            logger.error("Unexpected @context for sample %s: %s", sample_id, context)
    return metadata


def write(metadata: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Writing %s", output_path)
    jsonld_str = json.dumps(metadata, indent=2, ensure_ascii=False, default=str)
    output_path.write_text(jsonld_str, encoding="utf-8")


def fetch_raw_biosamples(config: BiosamplesConfig):
    logger.info("Starting biosamples extraction workflow")

    input_cfg = config.input
    excel_files = sorted(input_cfg.input_path.glob("*.xlsx")) + sorted(
        input_cfg.input_path.glob("*.xls")
    )
    if not excel_files:
        logger.error("No Excel files found in %s", input_cfg.input_path)
        return
    logger.info("Found %d Excel file(s) in %s", len(excel_files), input_cfg.input_path)

    for excel_file in tqdm(excel_files, desc="Excel files", unit="file"):
        logger.debug("Processing %s", excel_file.name)

        sample_ids = get_sample_ids(excel_file, input_cfg)
        if not sample_ids:
            continue

        logger.info("Found %d sample ID(s) in '%s'", len(sample_ids), excel_file.name)
        for sample_id in tqdm(sorted(sample_ids), desc=excel_file.name, unit="sample"):
            output_path = config.output.output_path / f"{sample_id}.jsonld"
            if output_path.exists():
                logger.debug("Skipping %s, already exists", sample_id)
                continue
            logger.debug("Fetching metadata for sample %s", sample_id)
            try:
                metadata = get_metadata(sample_id)
            except Exception as e:
                logger.error(
                    "Could not fetch metadata for sample '%s' from '%s': %s",
                    sample_id,
                    excel_file.name,
                    e,
                )
                continue

            metadata = modify_context(metadata, sample_id)
            write(metadata, output_path=output_path)

    logger.info("Biosamples extraction complete. Output: %s", config.output.output_path)


def uplift_biosamples(config: SourceConfig):
    raw_files = list(config.input_path.glob("**/*.jsonld"))
    logger.debug("Found %d raw file(s) in %s", len(raw_files), config.input_path)
    logger.debug("Found: %s", raw_files)
    for path in tqdm(raw_files, desc="Uplifting samples", unit="sample"):
        with path.open() as f:
            raw = json.load(f)
            logger.debug("Processing %s: %s", path.name, raw)
        try:
            uplifter = SampleUplifter(raw)
            product_dict, action_dict = uplifter.build_dicts()
        except Exception as e:
            logger.error("Failed to uplift %s: %s", path.name, e)
            continue
        try:
            product = Product(**product_dict)
            load_to_jsonld(product, output_path=config.output_path)
            try:
                make_strict(Product).model_validate(product_dict)
            except ValidationError as e:
                logger.warning(
                    "Strict validation failed for Product from %s", path.name
                )
                _log_validation_error(e, logger, level="warning")
        except ValidationError as e:
            logger.error("Failed to build Product for %s.", path.name)
            _log_validation_error(e, logger)
        try:
            action = Action(**action_dict)
            load_to_jsonld(action, output_path=config.output_path)
            try:
                make_strict(Action).model_validate(action_dict)
            except ValidationError as e:
                logger.warning("Strict validation failed for Action from %s", path.name)
                _log_validation_error(e, logger, level="warning")
        except ValidationError as e:
            logger.error("Failed to build Action for %s.", path.name)
            _log_validation_error(e, logger)
