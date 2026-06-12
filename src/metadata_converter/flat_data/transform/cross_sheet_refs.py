"""Collect and inject in-sheet cross-references during ingest.

Two-phase: ``collect_cross_ref_ids`` reads @id lists from the still-wide-format
DataFrames (where ``filter_column`` and ``@id`` are real columns), and
``inject_cross_refs`` later wires those references into the already-built
schema.org models.
"""

import logging

import pandas as pd

from metadata_converter.config import CrossSheetRef, FlatDataConfig
from metadata_converter.flat_data.uplifting import to_lookup_key
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)

# (rule, ref_type, collected @id strings)
CollectedRef = tuple[CrossSheetRef, str, list[str]]


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
