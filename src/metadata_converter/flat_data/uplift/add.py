"""AddApplier: set a property to a fixed constant value on every entity of a type.

For each ``AdditionRule`` the constant is built once, then assigned to every entity
of ``rule.on_type``, overwriting any existing value (and logging when it does).

The constant is either a *literal* (a scalar DataType value, set as-is) or a *node*:
a mapping carrying a ``type`` key that builds a typed schema.org object, recursively.
Models are built in non-strict mode so an unknown field
is kept; fields not declared on the resolved model are flagged with a warning but
retained. An unknown ``type`` cannot be built and raises.
"""

import logging
from typing import Any

from pydantic import BaseModel

from metadata_converter.config import AdditionRule
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import validate_strict

logger = logging.getLogger(__name__)


class AddApplier:
    """Apply ``AdditionRule``s to entities held by an ``EntityStore``."""

    def __init__(self, store: EntityStore) -> None:
        self.store = store

    def apply_all(self, rules: list[AdditionRule]) -> None:
        for rule in rules:
            self.apply(rule)

    def apply(self, rule: AdditionRule) -> None:
        built = self._build_value(rule, rule.value)

        added_count = 0
        for entity in self.store.of_type(rule.on_type):
            if getattr(entity, rule.target_property, None) is not None:
                logger.warning(
                    "Addition %s.%s: overwriting existing value on entity %r.",
                    rule.on_type, rule.target_property, entity.id,
                )
            setattr(entity, rule.target_property, built)
            added_count += 1

        logger.info(
            "Addition %s.%s: set %d entities.",
            rule.on_type, rule.target_property, added_count,
        )

    def _build_value(self, rule: AdditionRule, value: Any) -> Any:
        """Build a constant from config: scalar as-is, list element-wise, dict as a node."""
        if isinstance(value, list):
            return [self._build_value(rule, item) for item in value]
        if isinstance(value, dict):
            return self._build_node(rule, value)
        return value

    def _build_node(self, rule: AdditionRule, data: dict[str, Any]) -> BaseModel:
        """Build a typed schema.org object from a mapping carrying a ``type`` key."""
        type_name = data.get("type")
        if type_name is None:
            raise ValueError(
                f"AdditionRule {rule.on_type}.{rule.target_property}: a node value "
                f"must carry a 'type' key; got {data!r}."
            )
        try:
            cls = get_schema(type_name)
        except KeyError:
            raise ValueError(
                f"AdditionRule {rule.on_type}.{rule.target_property}: "
                f"unknown class name {type_name!r}."
            )

        fields = {
            key: self._build_value(rule, sub)
            for key, sub in data.items()
            if key != "type"
        }

        instance = cls(**fields)
        try:
            validate_strict(instance)
        except ValueError as e:
            logger.warning(
                "Addition %s.%s: value for type %r does not strictly validate against "
                "the schema.org model — keeping it, but it may not be intended. %s",
                rule.on_type, rule.target_property, type_name, e,
            )
        return instance
