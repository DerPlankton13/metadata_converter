import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from boltons.iterutils import remap
from pydantic import ValidationError
from tqdm import tqdm

from metadata_converter.biosamples.config import (
    BiosamplesConfig,
    BiosamplesExtractorConfig,
    BiosamplesUpliftConfig,
)
from metadata_converter.biosamples.fetch import (
    fuse_metadata,
    get_metadata,
    sample_source_urls,
)
from metadata_converter.biosamples.uplifting import SampleUplifter
from metadata_converter.load import load_to_jsonld, standardise_context
from metadata_converter.schema_org_models.schemaorg_models import (
    Action,
    Product,
    validate_strict,
)
from metadata_converter.utils.http import make_session
from metadata_converter.utils.io import write_json
from metadata_converter.utils.jsonld import expand_curie, standardise_id
from metadata_converter.utils.log_setup import log_validation_error
from metadata_converter.utils.provenance_writer import write_provenance_file

logger = logging.getLogger(__name__)


def get_sample_ids(
    excel_file: Path, config: BiosamplesExtractorConfig
) -> set[str] | None:
    df = pd.read_excel(
        excel_file,
        sheet_name=config.sheet_name,
        header=config.header,
        skiprows=config.skiprows,
    )
    df = df.dropna(how="all")

    if config.sample_id_column not in df.columns:
        logger.error(
            f"Column '{config.sample_id_column}' not found in sheet '{config.sheet_name}' of '{excel_file.name}'. Skipping this file now."
        )
        return None

    return set(df[config.sample_id_column].dropna().tolist())


def fetch_sample(sample_id: str, fetched_path: Path, config: BiosamplesConfig) -> bool:
    session = make_session(config.fetcher.user_agent)
    try:
        get_metadata(sample_id, session, fetched_path, config.provenance_dir)
        return True
    except Exception as e:
        logger.error("Could not fetch sample '%s': %s", sample_id, e)
        return False


def fetch_biosamples(config: BiosamplesConfig):
    logger.info("Starting biosamples fetch")

    input_cfg = config.extractor
    excel_files = sorted(input_cfg.input.glob("*.xlsx")) + sorted(
        input_cfg.input.glob("*.xls")
    )
    if not excel_files:
        logger.error("No Excel files found in %s", input_cfg.input)
        return
    logger.info("Found %d Excel file(s) in %s", len(excel_files), input_cfg.input)

    all_sample_ids: set[str] = set()
    for excel_file in excel_files:
        sample_ids = get_sample_ids(excel_file, input_cfg)
        if sample_ids:
            logger.info(
                "Found %d sample ID(s) in '%s'", len(sample_ids), excel_file.name
            )
            all_sample_ids.update(sample_ids)

    if not all_sample_ids:
        logger.warning("No sample IDs found across all files")
        return

    fetched_path = config.fetched_dir
    fetched_path.mkdir(parents=True, exist_ok=True)

    already_fetched = {
        sid
        for sid in all_sample_ids
        if (fetched_path / f"{sid}.ldjson").exists()
        and (fetched_path / f"{sid}.json").exists()
    }
    pending = all_sample_ids - already_fetched
    if already_fetched:
        logger.info("Skipping %d already-fetched sample(s)", len(already_fetched))
    if not pending:
        logger.info("All samples already fetched")
        return

    logger.info(
        "Fetching %d sample(s) with %d worker(s)",
        len(pending),
        config.fetcher.max_workers,
    )

    with ThreadPoolExecutor(max_workers=config.fetcher.max_workers) as executor:
        submitted = [
            executor.submit(fetch_sample, sid, fetched_path, config) for sid in pending
        ]
        failures = 0
        try:
            for future in tqdm(
                as_completed(submitted),
                total=len(submitted),
                desc="Fetching samples metadata",
                unit="sample",
                file=sys.stdout,
            ):
                if not future.result():
                    failures += 1
        except KeyboardInterrupt:
            logger.info("Interrupted — cancelling pending fetches")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

    if failures:
        raise RuntimeError(
            f"{failures} of {len(pending)} sample(s) failed to fetch — "
            "check the log for details and rerun to retry"
        )

    logger.info("Biosamples fetch complete. Output: %s", fetched_path)


def load_sample(structured: dict, unstructured: dict) -> dict:
    """Fuse a sample's structured and unstructured metadata and repair known source bugs."""
    fused = fuse_metadata(structured, unstructured)
    return fix_obi(fused)


def load_biosamples(config: BiosamplesConfig):
    logger.info("Starting biosamples load")

    fetched_path = config.fetched_dir
    ldjson_files = list(fetched_path.glob("*.ldjson"))
    logger.info("Found %d fetched sample(s) in %s", len(ldjson_files), fetched_path)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    failures = 0
    for ldjson_path in tqdm(
        ldjson_files, desc="Loading biosamples", unit="sample", file=sys.stdout
    ):
        sample_id = ldjson_path.stem
        json_path = fetched_path / f"{sample_id}.json"
        if not json_path.exists():
            logger.error("Missing unstructured metadata for %s, skipping", sample_id)
            failures += 1
            continue

        try:
            with ldjson_path.open() as f:
                structured = json.load(f)
            with json_path.open() as f:
                unstructured = json.load(f)
            sample = load_sample(structured, unstructured)
        except Exception as e:
            logger.error("Failed to load %s: %s", sample_id, e)
            failures += 1
            continue

        sample = standardise_context(sample)
        sample = standardise_id(sample)

        write_json(sample, config.output_dir / sample["@id"])
        if config.provenance_dir is not None:
            structured_id = expand_curie(structured["@id"], structured["@context"])
            write_provenance_file(
                sample["@id"],
                config.provenance_dir,
                [structured_id, os.path.relpath(json_path)],
                "load",
            )

    if failures:
        raise RuntimeError(
            f"{failures} of {len(ldjson_files)} sample(s) failed to load — "
            "check the log for details"
        )
    logger.info("Biosamples load complete. Output: %s", config.output_dir)


def uplift_biosamples(config: BiosamplesUpliftConfig):
    logger.info("Starting biosamples uplift")

    files = list(config.input_dir.glob("**/*.jsonld"))
    logger.info("Found %d loaded file(s) in %s", len(files), config.input_dir)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    failures = 0
    for path in tqdm(
        files, desc="Uplifting biosamples", unit="sample", file=sys.stdout
    ):
        with path.open() as f:
            data = json.load(f)
        try:
            uplifter = SampleUplifter(data)
            product_dict, action_dict = uplifter.build_dicts()
        except Exception as e:
            logger.error("Failed to uplift %s: %s", path.name, e)
            failures += 1
            continue
        try:
            product = Product(**product_dict)
            load_to_jsonld(product, output_dir=config.output_dir)
            if config.provenance_dir is not None:
                write_provenance_file(
                    product.id, config.provenance_dir, data["@id"], "uplift"
                )
            try:
                validate_strict(product)
            except ValueError as e:
                logger.warning(
                    "Strict validation failed for Product from %s: %s", path.name, e
                )
        except ValidationError as e:
            logger.error("Failed to build Product for %s.", path.name)
            log_validation_error(e, logger)
            failures += 1
        try:
            action = Action(**action_dict)
            load_to_jsonld(action, output_dir=config.output_dir)
            if config.provenance_dir is not None:
                write_provenance_file(
                    action.id, config.provenance_dir, data["@id"], "uplift"
                )
            try:
                validate_strict(action)
            except ValueError as e:
                logger.warning(
                    "Strict validation failed for Action from %s: %s", path.name, e
                )
        except ValidationError as e:
            logger.error("Failed to build Action for %s.", path.name)
            log_validation_error(e, logger)
            failures += 1

    if failures:
        logger.warning(
            "Biosamples uplift completed with %d failure(s) out of %d sample(s) — "
            "check the log for details",
            failures,
            len(files),
        )
    else:
        logger.info("Biosamples uplift complete. Output: %s", config.output_dir)


def fix_obi(fused: dict) -> dict:
    """Expand OBI compact IRIs to full https IRIs and drop the OBI context entry.

    JSON-LD 1.1 only auto-expands a compact IRI like "OBI:0000747" when the
    prefix's mapped IRI ends in a URI gen-delim character (e.g. ":", "/"); OBI's
    mapped IRI ends in "_", so it no longer expands. Rewrite it to the full IRI
    instead, to make the output JSON-LD version independent.
    """
    try:
        obi_iri = fused["@context"][1].pop("OBI", None)
    except (KeyError, IndexError, TypeError):
        obi_iri = None
    if obi_iri is None:
        return fused

    def replace_obi(path, key, value):
        if isinstance(value, str) and value.startswith("OBI:"):
            return key, obi_iri + value.removeprefix("OBI:")
        return key, value

    return remap(fused, visit=replace_obi)
