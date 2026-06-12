"""Collect and inject in-sheet cross-references during ingest.

Two-phase: ``collect_cross_ref_ids`` reads @id lists from the still-wide-format
DataFrames (where ``filter_column`` and ``@id`` are real columns), and
``inject_cross_refs`` later wires those references into the already-built
schema.org models.
"""

import logging
from typing import Any

import pandas as pd

from metadata_converter.config import CrossSheetRef, FlatDataConfig
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)

# (rule, ref_type, collected @id strings)
CollectedRef = tuple[CrossSheetRef, str, list[str]]


def to_lookup_key(value: Any) -> str | None:
    """Convert a raw data value to the canonical string used for value matching.

    Both sides of a comparison — the value read from one source and the value
    read from the other — pass through this function before comparison. Using
    the same normalization on both sides makes matches type-independent.

    - ``None`` → ``None`` (caller skips these).
    - ``bool`` → ``"true"`` / ``"false"`` so that ``match_literal = "true"`` matches them.
    - ``float`` with an integer value (e.g. ``1.0``) → equivalent int string.
      Pydantic's smart-mode union resolution coerces ``int 1`` to ``float 1.0``
      when the target field's union prefers ``float``; this collapse lets
      ``match_literal = "1"`` still match such a value.
    - everything else → ``str(value).strip().lower()``.

    Also used by ``LinkEngine`` in ``uplifting.py`` for cross-file link matching.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip().lower()


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
