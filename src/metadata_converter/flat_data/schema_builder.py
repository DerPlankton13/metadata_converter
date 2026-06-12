"""Build schema.org Pydantic models from long-format entity dicts."""

import logging
from typing import Any

import pandas as pd
from pydantic import ValidationError

from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase

logger = logging.getLogger(__name__)


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
    """Build schema.org objects for one entity row, recursing into nested mapping values."""
    schema_type = mapping.get("type")
    if not schema_type:
        raise ValueError(
            f"Missing 'type' in {'nested ' if nested else ''}mapping: {mapping!r}"
        )

    props = extract_properties(
        entity, {k: v for k, v in mapping.items() if k != "type"}
    )
    if not props:
        return []

    rows = split_properties(props) if nested and is_multi_instance(props) else [props]
    return [
        s for s in (instantiate_schema(schema_type, r) for r in rows) if s is not None
    ]


def extract_properties(entity: dict[str, Any], mapping: dict) -> dict[Any, Any]:
    """Resolve a mapping against one entity dict to produce schema property values.

    String values are column lookups; ``"Literal:..."`` returns the suffix verbatim.
    Dict and list values trigger nested schema building recursively.
    """
    schema_properties = {}
    for prop, value in mapping.items():
        if isinstance(value, str):
            if value.startswith("Literal:"):
                schema_properties[prop] = value[len("Literal:") :]
                continue
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
    """True when all list-valued properties share the same length > 1."""
    lengths = {len(v) for v in props.values() if isinstance(v, list)}
    return len(lengths) == 1 and lengths.pop() > 1


def split_properties(props: dict[str, Any]) -> list[dict[str, Any]]:
    """Transpose parallel-list properties into one dict per row."""
    keys = list(props.keys())
    columns = [props[k] if isinstance(props[k], list) else [props[k]] for k in keys]
    return [dict(zip(keys, row)) for row in zip(*columns)]
