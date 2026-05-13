import json
import logging

import pandas as pd

from metadata_converter.biosamples.fetch import get_metadata
from metadata_converter.config import BiosamplesConfig

logger = logging.getLogger(__name__)


def run_biosamples_extraction(config: BiosamplesConfig):
    logger.info("Starting biosamples extraction workflow")
    input_cfg = config.input
    excel_files = sorted(input_cfg.input_path.glob("*.xlsx")) + sorted(input_cfg.input_path.glob("*.xls"))

    if not excel_files:
        logger.warning("No Excel files found in %s", input_cfg.input_path)
        return

    logger.info("Found %d Excel file(s) in %s", len(excel_files), input_cfg.input_path)

    for excel_file in excel_files:
        logger.info("Processing %s", excel_file.name)
        df = pd.read_excel(
            excel_file,
            sheet_name=input_cfg.sheet_name,
            header=input_cfg.header,
            skiprows=input_cfg.skiprows,
        )
        df = df.dropna(how="all")

        if input_cfg.header_name not in df.columns:
            raise KeyError(
                f"Column '{input_cfg.header_name}' not found in sheet '{input_cfg.sheet_name}' of '{excel_file.name}'"
            )
        sample_ids = set(df[input_cfg.header_name].dropna().tolist())
        logger.info("Found %d sample ID(s) in '%s'", len(sample_ids), excel_file.name)

        for i, sample_id in enumerate(sorted(sample_ids), 1):
            logger.info("[%d/%d] Fetching metadata for sample %s", i, len(sample_ids), sample_id)
            try:
                metadata = get_metadata(sample_id)
            except Exception as e:
                logger.error(
                    "Could not fetch metadata for sample '%s' from '%s': %s",
                    sample_id, excel_file.name, e,
                )
                continue

            context = metadata.get("@context")
            if not context:
                logger.warning("No '@context' found for sample %s", sample_id)
            else:
                try:
                    terms = context[1]
                    context = {"@vocab": "https://schema.org", **terms}
                    metadata["@context"] = context
                except (IndexError, TypeError):
                    logger.warning("Unexpected @context for sample %s: %s", sample_id, context)

            output_path = config.output.output_path / f"raw/{sample_id}.jsonld"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug("Writing %s", output_path)
            jsonld_str = json.dumps(metadata, indent=2, ensure_ascii=False, default=str)
            output_path.write_text(jsonld_str, encoding="utf-8")

    logger.info("Biosamples extraction complete. Output: %s", config.output.output_path)
