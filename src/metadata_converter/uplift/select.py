"""Selector helpers for navigating schema.org Pydantic models at uplift time.

Public helpers used by appliers to read values from entities without committing
to a specific dot-path syntax inside the applier itself.

Selector expressions
--------------------
A selector is a dot-separated chain of field names:

- ``identifier``       — read a single field
- ``about.identifier`` — descend into a nested object first

When the final step resolves to a PropertyValue-like model (one that carries a
non-``None`` ``value`` field), the engine dereferences to that value automatically.
This covers ``Orcid``, ``PropertyValue``, and similar schema.org wrappers — no
need to append ``.value`` at the end of a selector.
"""

import re
from typing import Any

from pydantic import BaseModel

from metadata_converter.schema_org_models.schemaorg_models import (
    PropertyValue,
    SchemaOrgBase,
)


def select_values(obj: Any, selector: str) -> list:
    """Return all values reached by walking ``selector`` on ``obj``.

    ``selector`` is a dot-separated chain of field names applied left-to-right,
    e.g. ``"agent.identifier"``. The result is always a list because any field
    along the path may hold a list of models — in that case the remaining selector
    is applied to every element and all results are merged::

        select_values(action, "agent.identifier")
        # → ["0000-0001-2345-6789"]  — even when there is only one agent

    When the final field resolves to a model that carries a ``value`` attribute
    (``Orcid``, ``PropertyValue``, …), that inner value is extracted automatically —
    no need to append ``.value`` to the selector.
    """
    if obj is None:
        return []
    if selector == "":
        return unwrap_value(obj)

    first_segment, _, remaining = selector.partition(".")

    if isinstance(obj, list):
        # Fan out: the full selector still needs to be applied to each element,
        # because we haven't consumed any segment yet — we're just spreading across items.
        results: list = []
        for item in obj:
            results.extend(select_values(item, selector))
        return results

    field_value = getattr(obj, first_segment, None)

    if remaining:
        return select_values(field_value, remaining)  # more segments to walk — recurse
    else:
        return unwrap_value(field_value)  # last segment reached — extract value


def unwrap_value(value: Any) -> list:
    """Flatten lists and extract ``.value`` from PropertyValue-like wrappers.

    Returns a list to keep callers branch-free. Specifically:

    - ``None`` → ``[]``
    - list → flattened, collecting all leaf values
    - Pydantic model with a non-``None`` ``value`` attribute → unwrap that value
      (this is the schema.org idiom for ``Orcid``, ``PropertyValue``, etc.)
    - anything else → ``[value]``
    """
    if value is None:
        return []
    if isinstance(value, list):
        results: list = []
        for item in value:
            results.extend(unwrap_value(item))
        return results
    if isinstance(value, BaseModel):
        inner = getattr(value, "value", None)
        if inner is not None:
            return unwrap_value(inner)
        return [value]
    return [value]


def render_ref_id(template: str, candidate: SchemaOrgBase) -> str | None:
    """Render a ref ``@id`` by substituting ``{prop}`` placeholders with candidate values.

    Returns ``None`` when any placeholder cannot be resolved on ``candidate``.
    """
    result = template
    for prop in re.findall(r"\{(\w+)\}", template):
        values = select_values(candidate, prop)
        if not values:
            return None
        result = result.replace(f"{{{prop}}}", str(values[0]))
    return result


def find_additional_property(entity: SchemaOrgBase, name: str) -> list:
    """Return unwrapped values from ``additionalProperty`` items matching ``name``."""
    ap = entity.additionalProperty
    if ap is None:
        return []
    items = ap if isinstance(ap, list) else [ap]
    results: list = []
    for item in items:
        if isinstance(item, PropertyValue) and item.name == name:
            results.extend(unwrap_value(item.value))
    return results
