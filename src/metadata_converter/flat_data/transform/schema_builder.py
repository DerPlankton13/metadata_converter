"""Build schema.org Pydantic models from long-format entity data.

The TOML mapping is polymorphic: an entry can be a constant, a column name,
a nested object, or a list of nested objects. To keep "what the mapping
declares" separate from "how we resolve a row against it", the mapping is
parsed once into a typed **AST** ("Abstract Syntax Tree" — the compiler-style
trick of representing structured input as a tree of typed objects where each
node names exactly what kind of thing it is). Four dataclasses below —
``Literal``, ``ColumnRef``, ``Nested``, ``NestedList`` — are the AST node types,
one per kind of mapping entry. Per-entity evaluation then just dispatches on
the typed node.

Mapping AST
-----------
- ``Literal(value)``       — constant; emits ``value`` verbatim.
- ``ColumnRef(name)``         — column lookup; emits the entity's value(s) for ``name``.
- ``Nested(type, fields)`` — nested schema; emits a sub-object of class ``type``,
                              with each field resolved by its own AST node.
- ``NestedList(items)``    — a list of nested objects (TOML ``[[block]]``); each
                              item is a ``Nested``; the field always carries a list.

Evaluation
----------
- ``build_root`` evaluates a top-level schema for one entity. Single instance,
  no fan-out; multi-value leaves become list-valued properties. Literal-only
  entities still emit (nothing to "fan out into" at the entry point).
- ``build_nested`` evaluates a nested schema. Multi-value leaves drive fan-out
  into N sub-objects; a schema with no row-derived data is dropped.

Internal invariants
-------------------
- Every column value lives in a ``list`` inside the walker. Empty = missing.
- Singleton lists collapse to scalars at field assignment for ``Nested``
  sub-fields. ``NestedList`` sub-fields never collapse — list is their declared shape.
"""

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pydantic import ValidationError

from metadata_converter.config import FlatDataConfig
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)

LITERAL_PREFIX = "Literal:"


# ---------------------------------------------------------------------------
# Mapping AST: typed representation of the TOML mapping config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Literal:
    """Constant value to emit regardless of the row."""

    value: str


@dataclass(frozen=True)
class ColumnRef:
    """Reference to a column in the entity; emits the row's value(s)."""

    name: str


@dataclass
class Nested:
    """Nested schema: emit a sub-object of class ``type`` from these field mappings."""

    type: str
    fields: dict[str, "Node"]


@dataclass
class NestedList:
    """A list of nested objects (TOML ``[[block]]`` syntax); always emitted as a list."""

    items: list[Nested]


Node = Literal | ColumnRef | Nested | NestedList


def parse_mapping(raw: Any) -> Node:
    """Parse a raw TOML mapping fragment into a typed AST.

    Malformed mappings raise here — once, at startup — instead of failing per
    entity row when the data evaluation hits them.
    """
    if isinstance(raw, str):
        if raw.startswith(LITERAL_PREFIX):
            return Literal(value=raw[len(LITERAL_PREFIX) :])
        return ColumnRef(name=raw)
    if isinstance(raw, dict):
        if "type" not in raw:
            raise ValueError(f"Missing 'type' in mapping: {raw!r}")
        return Nested(
            type=raw["type"],
            fields={k: parse_mapping(v) for k, v in raw.items() if k != "type"},
        )
    if isinstance(raw, list):
        items: list[Nested] = []
        for item in raw:
            node = parse_mapping(item)
            if not isinstance(node, Nested):
                raise TypeError(f"Mapping list elements must be schemas; got {node!r}.")
            items.append(node)
        return NestedList(items=items)
    raise TypeError(f"Unsupported mapping node: {raw!r}")


# ---------------------------------------------------------------------------
# Evaluation: walk the AST against per-entity data
# ---------------------------------------------------------------------------


def build_schemas(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, list[SchemaOrgBase]]:
    """Build schema.org Pydantic models for every entity in every sheet."""
    mappings = {sheet: parse_mapping(config.mapping[sheet]) for sheet in data_dict}
    results: dict[str, list[SchemaOrgBase]] = {}
    for sheet, df in data_dict.items():
        logger.info("Building schemas for sheet '%s'", sheet)
        results[sheet] = build_sheet(df, mappings[sheet])
    return results


def build_sheet(df: pd.DataFrame, mapping: Nested) -> list[SchemaOrgBase]:
    """Build one schema.org instance per entity in this sheet."""
    schemas: list[SchemaOrgBase] = []
    for _, group in df.groupby("id"):
        row = pivot_row(group)
        schemas.extend(build_root(mapping, row))
    return schemas


def pivot_row(group: pd.DataFrame) -> dict[str, list[Any]]:
    """Pivot one entity's long-format slice into ``{column_name: [values...]}``."""
    return group.groupby("header")["value"].apply(list).to_dict()


def build_root(mapping: Nested, row: dict[str, list[Any]]) -> list[SchemaOrgBase]:
    """Evaluate a top-level mapping: single instance, no fan-out, literals-only OK."""
    column, nested, literal = resolve_fields(mapping, row)
    cls = get_schema(mapping.type)
    column = {k: unwrap_single(v) for k, v in column.items()}
    return instantiate(cls, kwargs={**literal, **nested, **column})


def resolve_fields(
    mapping: Nested, row: dict[str, list[Any]]
) -> tuple[dict[str, list[Any]], dict[str, Any], dict[str, str]]:
    """Evaluate a mapping's fields against one row, sorted by output role.

    Returns ``(column, nested, literal)``:

    - ``column``: column-derived value lists (drive fan-out cardinality).
    - ``nested``: sub-object results, already shaped (``Nested`` branches collapsed
      to scalar when length 1; ``NestedList`` branches kept as lists).
    - ``literal``: constants from ``Literal`` nodes (broadcast across instances).
    """
    column: dict[str, list[Any]] = {}
    nested: dict[str, Any] = {}
    literal: dict[str, str] = {}
    for prop, sub in mapping.fields.items():
        match sub:
            case Literal(value=v):
                literal[prop] = v
            case ColumnRef(name=name):
                values = resolve_column(name, row)
                if values:
                    column[prop] = values
            case Nested():
                items = build_nested(sub, row)
                if items:
                    nested[prop] = unwrap_single(items)
            case NestedList(items=blocks):
                items = []
                for block in blocks:
                    items.extend(build_nested(block, row))
                if items:
                    nested[prop] = items
    return column, nested, literal


def resolve_column(column: str, row: dict[str, list[Any]]) -> list[Any]:
    """Read a column's values from the row, dropping NaN entries."""
    if column not in row:
        raise KeyError(f"Header '{column}' not found in the data.")
    return [v for v in row[column] if pd.notna(v)]


def build_nested(mapping: Nested, row: dict[str, list[Any]]) -> list[SchemaOrgBase]:
    """Evaluate a nested mapping into zero or more Pydantic instances.

    A single mapping can produce multiple instances when one of its column
    fields holds multiple values — each value becomes its own sub-object,
    with the other fields (literals, single-value columns, nested sub-objects)
    broadcast across every instance. This is what makes a multi-value cell
    like ``"a, b"`` end up as two ``PropertyValue`` siblings rather than one
    PropertyValue with a list inside.

    Rules
    -----
    - **No data of any kind** (no columns resolved, no nested sub-results,
      no literals): return ``[]``.
    - **Literal-only mapping** (no column refs declared anywhere in the
      subtree): the literals are the entire content; emit one constant instance
      regardless of the row.
    - **Literals alongside an empty column** (column refs declared but all
      came back empty for this row): the literals were decoration for missing
      data; return ``[]``.
    - **One value per column** (or only literals/nested): return one instance.
    - **N values in one or more columns**: return N instances. Columns with
      length N contribute their i-th value to the i-th instance; columns with
      one value broadcast that value to all instances.
    - **Inconsistent column lengths** (some > 1 but not equal to the max):
      lengths can't be aligned; log a warning and return ``[]``.

    Example
    -------
    Mapping ``{type: "PropertyValue", name: "Literal:label", value: "col"}``
    against a row where ``col = ["a", "b"]`` produces two PropertyValues:
    one with ``value="a"`` and one with ``value="b"`` — both with the same
    broadcast ``name="label"``.
    """
    column, nested, literal = resolve_fields(mapping, row)
    # Literals carry content only when the mapping itself doesn't depend on the row.
    content_literals = literal if not _reads_row_data(mapping) else {}
    if not (column or nested or content_literals):
        return []

    n = max((len(v) for v in column.values()), default=1)
    mismatched = {k: len(v) for k, v in column.items() if len(v) not in (1, n)}
    if mismatched:
        logger.warning(
            "Cannot align %s: column fields %s have lengths inconsistent with "
            "max length %d; skipping",
            mapping.type,
            mismatched,
            n,
        )
        return []

    cls = get_schema(mapping.type)
    instances: list[SchemaOrgBase] = []
    for i in range(n):
        per_instance_columns = {
            k: v[i] if len(v) == n else v[0] for k, v in column.items()
        }
        kwargs = {**literal, **nested, **per_instance_columns}
        instances.extend(instantiate(cls, kwargs))
    return instances


def _reads_row_data(mapping: Nested) -> bool:
    """True if ``mapping`` has any ``ColumnRef`` anywhere in its subtree.

    Distinguishes ``{type: PropertyValue, name: "Literal:flag", value: "col"}``
    (declares a column ref; literals are decoration) from
    ``{type: MonetaryGrant, id: "Literal:..."}`` (purely literal, no row
    dependency). The former is dropped when its column is empty; the latter
    is emitted regardless of row.
    """
    for sub in mapping.fields.values():
        match sub:
            case ColumnRef():
                return True
            case Nested():
                if _reads_row_data(sub):
                    return True
            case NestedList(items=blocks):
                if any(_reads_row_data(b) for b in blocks):
                    return True
    return False


def unwrap_single(items: list) -> Any:
    """Return ``items[0]`` for a single-value list; the list as-is otherwise.

    Internal field values are kept as lists for uniform handling; this collapses
    them at the boundary so the output matches JSON-LD convention (``"name": "x"``
    instead of ``"name": ["x"]``).
    """
    return items[0] if len(items) == 1 else items


def instantiate(cls: type[SchemaOrgBase], kwargs: dict) -> list[SchemaOrgBase]:
    """Instantiate ``cls`` with ``kwargs``; on validation error log per-error and skip."""
    try:
        return [cls(**kwargs)]
    except ValidationError as e:
        for err in e.errors():
            logger.warning(
                "Could not create %s: %s at %s (input: %s)",
                cls.__name__,
                err["msg"],
                err["loc"],
                err.get("input"),
            )
        logger.debug("Kwargs provided: %s", kwargs)
        return []
