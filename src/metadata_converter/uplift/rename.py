"""RenameApplier: move a property's value to a different name on every entity of a type.

For each ``RenameRule``, the applier reads ``source_property`` on every entity of
``rule.on_type``. When the source has a value, it is moved to ``target_property``
(overwriting any existing value there, and logging when it does) and
``source_property`` is cleared. Entities with no value at ``source_property`` are
left untouched.
"""

import logging

from metadata_converter.uplift.config import RenameRule
from metadata_converter.uplift.entity_store import EntityStore

logger = logging.getLogger(__name__)


class RenameApplier:
    """Apply ``RenameRule``s to entities held by an ``EntityStore``."""

    def __init__(self, store: EntityStore) -> None:
        self.store = store

    def apply_all(self, rules: list[RenameRule]) -> None:
        for rule in rules:
            self.apply(rule)

    def apply(self, rule: RenameRule) -> None:
        renamed_count = 0
        for entity in self.store.of_type(rule.on_type):
            value = getattr(entity, rule.source_property, None)
            if value is None:
                continue
            existing = getattr(entity, rule.target_property, None)
            if existing is not None:
                logger.warning(
                    "Rename %s.%s → %s: overwriting existing value %r on entity %r.",
                    rule.on_type, rule.source_property, rule.target_property, existing, entity.id,
                )
            setattr(entity, rule.target_property, value)
            setattr(entity, rule.source_property, None)
            renamed_count += 1

        logger.info(
            "Rename %s.%s → %s: moved %d entities.",
            rule.on_type, rule.source_property, rule.target_property, renamed_count,
        )
