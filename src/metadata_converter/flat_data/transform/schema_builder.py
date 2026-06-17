"""Build schema.org Pydantic models from long-format entity dicts."""

import logging
from typing import Any

import pandas as pd
from pydantic import ValidationError

from metadata_converter.config import FlatDataConfig
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)


def build_schemas(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, list[SchemaOrgBase]]:
    """Build schema.org objects from each sheet's long-format DataFrame."""
    results = {}
    for name, data in data_dict.items():
        logger.info("Building schemas for sheet '%s'", name)
        results[name] = extract_schemas(data, config.mapping[name])
    return results


def extract_schemas(df: pd.DataFrame, mapping: dict[str, Any]) -> list[SchemaOrgBase]:
    """Convert a long-format DataFrame into schema.org Pydantic model instances."""
    schemas = []
    groups = list(df.groupby("id"))
    logger.info("Building schemas for %d record(s) ...", len(groups))
    for _, entity in groups:
        entity = entity.groupby("header")["value"].apply(list).to_dict()
        result = build_schema(entity, mapping)
        if result:
            schemas.extend(result)
    logger.info("Built %d schema(s) successfully", len(schemas))
    return schemas


def build_schema(
    entity: dict[str, Any], mapping: dict, nested: bool = False
) -> list[SchemaOrgBase]:
    """Build schema.org instance(s) from one entity row according to ``mapping``.

    A mapping carries two kinds of property entries:

    - **Literals** (``"Literal:<value>"`` strings) are constants — the substring
      after the prefix is emitted verbatim regardless of the row.
    - **Data entries** (column names, nested dicts, lists of dicts) pull values
      from ``entity``.

    Literals decorate data and never gate emission on their own: a nested
    schema is dropped when no data entry resolves, even if its literals would
    have produced a stub. Top-level entities pass through with literals only.

    Multi-valued data in a nested mapping fans out into N instances
    (one per value); scalar data and literals are broadcast across them.
    """
    schema_type = mapping.get("type")
    if not schema_type:
        raise ValueError(
            f"Missing 'type' in {'nested ' if nested else ''}mapping: {mapping!r}"
        )

    literal_props, data_mapping = partition_mapping(mapping)
    data_props = extract_properties(entity, data_mapping)
    if nested and not data_props:
        return []

    props = {**literal_props, **data_props}
    if not props:
        return []

    rows = split_properties(props) if nested and is_multi_instance(props) else [props]
    return [
        s for s in (instantiate_schema(schema_type, r) for r in rows) if s is not None
    ]


def partition_mapping(
    mapping: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    """Split a mapping into resolved literal values and remaining data entries.

    The ``"type"`` key is schema-class metadata and excluded from both outputs.
    ``"Literal:<value>"`` entries are returned with the prefix stripped — the
    constant value to emit. Every other entry (column names, nested dicts,
    lists) passes through unchanged for ``extract_properties`` to resolve.
    """
    literals: dict[str, str] = {}
    data: dict[str, Any] = {}
    for key, value in mapping.items():
        if key == "type":
            continue
        if isinstance(value, str) and value.startswith("Literal:"):
            literals[key] = value[len("Literal:") :]
        else:
            data[key] = value
    return literals, data


def extract_properties(entity: dict[str, Any], mapping: dict) -> dict[Any, Any]:
    """Read row values from ``entity`` according to ``mapping``.

    String mapping values are column names looked up in ``entity``; dict and
    list values trigger recursive ``build_schema`` calls. Entries whose lookup
    yields no value are omitted from the result.
    """
    schema_properties = {}
    for prop, value in mapping.items():
        if isinstance(value, str):
            var = get_field_value(entity, value)
            if var is not None:
                schema_properties[prop] = var
        elif isinstance(value, dict):
            nested = build_schema(entity, value, nested=True)
            if nested:
                schema_properties[prop] = nested[0] if len(nested) == 1 else nested
        elif isinstance(value, list):
            for schema_mapping in value:
                if not isinstance(schema_mapping, dict):
                    raise TypeError(
                        f"Mapping list elements must be dicts; got {schema_mapping!r}."
                    )
                nested = build_schema(entity, schema_mapping, nested=True)
                if nested:
                    schema_properties.setdefault(prop, []).extend(nested)
    return schema_properties


def instantiate_schema(
    schema_type: str, schema_properties: dict
) -> SchemaOrgBase | None:
    """Instantiate a schema.org Pydantic model; log and return None on ValidationError."""
    try:
        return get_schema(schema_type)(**schema_properties)
    except ValidationError as e:
        for err in e.errors():
            logger.warning(
                "Could not create %s: %s at %s (input: %s)",
                schema_type,
                err["msg"],
                err["loc"],
                err.get("input"),
            )
        logger.debug("Properties provided: %s", schema_properties)
        return None


def get_field_value(entity: dict[str, Any], column: str):
    """Return field value(s) from entity, stripping NAs; None if all values are missing."""
    if column not in entity:
        raise KeyError(f"Header '{column}' not found in the data.")
    values = entity[column]
    if not isinstance(values, list):
        values = [values]
    values = [v for v in values if pd.notna(v)]
    if not values:
        return None
    return values[0] if len(values) == 1 else values


def is_multi_instance(props: dict[str, Any]) -> bool:
    """True when ``props`` should fan out into multiple instances via ``split_properties``.

    Triggers when every list-valued property has the same length > 1, signalling
    parallel data that pairs element-wise.
    """
    lengths = {len(v) for v in props.values() if isinstance(v, list)}
    return len(lengths) == 1 and lengths.pop() > 1


def split_properties(props: dict[str, Any]) -> list[dict[str, Any]]:
    """Transpose parallel-list properties into one dict per row.

    List-valued properties are zipped element-wise; scalar properties (literals
    or single-value column lookups) are broadcast — repeated across every row
    so they appear on each emitted instance.
    """
    n = max(
        (len(v) for v in props.values() if isinstance(v, list)),
        default=1,
    )
    columns = [
        props[k] if isinstance(props[k], list) else [props[k]] * n for k in props
    ]
    return [dict(zip(props.keys(), row)) for row in zip(*columns)]
