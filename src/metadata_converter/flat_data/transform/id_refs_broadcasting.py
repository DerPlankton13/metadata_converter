"""Collect and inject in-sheet cross-references during ingest.

Two-phase: ``prepare_id_ref_broadcast`` reads @id lists from the still-wide-format
DataFrames (where ``filter_column`` and ``@id`` are real columns), and
``broadcast_id_refs`` later wires those references into the already-built
schema.org models.
"""

import logging
from typing import Any

import pandas as pd

from metadata_converter.config import BroadcastIdRef, FlatDataConfig
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)

# (rule, ref_type, collected @id strings)
Broadcast = tuple[BroadcastIdRef, str, list[str]]


def extract_inline_id_ref_broadcasts(config: FlatDataConfig) -> None:
    """Lift inline ``id = { from_sheet = ... }`` mapping entries into ``config.broadcast_id_refs``.

    Mutates ``config.mapping`` and ``config.broadcast_id_refs`` in place. Idempotent —
    on a second call there are no inline entries left to extract.

    A property whose value matches the shape::

        { type = "<Type>", id = { from_sheet = "<sheet>", filter_column = "...", filter_value = ... } }

    is removed from the mapping and re-expressed as a ``BroadcastIdRef``. The schema
    builder then sees only embedded sub-objects and column refs.
    """
    for sheet_name, sheet_mapping in config.mapping.items():
        for prop in list(sheet_mapping.keys()):
            ref = _try_extract(sheet_mapping[prop], sheet_name, prop, config.mapping)
            if ref is not None:
                config.broadcast_id_refs.append(BroadcastIdRef(**ref))
                del sheet_mapping[prop]


def _try_extract(
    value: Any, sheet_name: str, prop: str, mapping: dict
) -> dict | None:
    """Return a ``BroadcastIdRef``-shaped dict if ``value`` is an inline broadcast @id ref, else None.

    Raises ``ValueError`` if the shape is clearly intended-as-ref but malformed.
    """
    if not isinstance(value, dict):
        return None
    id_spec = value.get("id")
    if not isinstance(id_spec, dict) or "from_sheet" not in id_spec:
        return None

    # From here, the user clearly intended a broadcast @id ref. Strict-validate.
    path = f"{sheet_name}.{prop}"

    if "type" not in value:
        raise ValueError(f"{path}: inline broadcast @id ref is missing `type`.")
    outer_extras = set(value) - {"type", "id"}
    if outer_extras:
        raise ValueError(
            f"{path}: inline broadcast @id ref must have exactly `type` and `id`; "
            f"got unexpected keys {sorted(outer_extras)}."
        )

    inner_extras = set(id_spec) - {"from_sheet", "filter_column", "filter_value"}
    if inner_extras:
        raise ValueError(
            f"{path}.id: unexpected keys {sorted(inner_extras)}; "
            f"allowed: from_sheet, filter_column, filter_value."
        )
    if (id_spec.get("filter_column") is None) != (id_spec.get("filter_value") is None):
        raise ValueError(
            f"{path}.id: filter_column and filter_value must be provided together."
        )

    from_sheet = id_spec["from_sheet"]
    if from_sheet not in mapping:
        raise ValueError(
            f"{path}.id.from_sheet: unknown sheet '{from_sheet}'. "
            f"Known sheets: {sorted(mapping)}."
        )
    declared = value["type"]
    target = (
        mapping[from_sheet].get("type") if isinstance(mapping[from_sheet], dict) else None
    )
    if target is not None and declared != target:
        raise ValueError(
            f"{path}: declared type '{declared}' does not match "
            f"mapping['{from_sheet}'].type ('{target}')."
        )

    return {
        "on_sheet": sheet_name,
        "property": prop,
        "from_sheet": from_sheet,
        "filter_column": id_spec.get("filter_column"),
        "filter_value": id_spec.get("filter_value"),
    }


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


def prepare_id_ref_broadcast(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> list[Broadcast]:
    """Collect @id lists for each broadcast @id ref rule while data is still wide-format.

    Wide format is required because filter_column and @id are still actual columns here.
    The ref_type is captured now so the injection step needs no access to the config.
    """
    collected: list[Broadcast] = []
    for ref in config.broadcast_id_refs:
        src = data_dict[ref.from_sheet]
        if ref.filter_column is not None:
            filter_key = to_lookup_key(ref.filter_value)
            src = src[src[ref.filter_column].map(to_lookup_key) == filter_key]
        ids = src["@id"].dropna().tolist()
        ref_type = config.mapping[ref.from_sheet]["type"]
        collected.append((ref, ref_type, ids))
    return collected


def broadcast_id_refs(
    results: dict[str, list[SchemaOrgBase]],
    collected: list[Broadcast],
) -> dict[str, list[SchemaOrgBase]]:
    """Inject pre-collected broadcast @id references into already-built schemas."""
    for ref, ref_type, ids in collected:
        if not ids:
            logger.warning(
                "broadcast_id_refs: no sources for %s.%s", ref.on_sheet, ref.property
            )
            continue
        ref_cls = get_schema(ref_type)
        refs = [ref_cls(id=i) for i in ids]
        value = refs if len(refs) > 1 else refs[0]
        for schema in results.get(ref.on_sheet, []):
            setattr(schema, ref.property, value)
    return results
