"""RemoveApplier: filter items out of a list-valued property by a predicate.

For each ``RemovalRule``, the applier finds entities of ``rule.on_type``, reads
``rule.target_property``, and removes items matching ``rule.where``. The predicate
reads a (possibly nested) subproperty of each item via the shared selector and
compares on string form (case-sensitive). An item matches when *any* resolved
value satisfies the predicate.

Output lists are kept as short as possible: an emptied collection collapses to
``None``, a lone survivor collapses to a single value, and only two-or-more
survivors are written back as a list.
"""

import logging

from metadata_converter.config import RemovalRule, RemovalWhere
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.flat_data.uplift.select import select_values

logger = logging.getLogger(__name__)


class RemoveApplier:
    """Apply ``RemovalRule``s to entities held by an ``EntityStore``."""

    def __init__(self, store: EntityStore) -> None:
        self.store = store

    def apply_all(self, rules: list[RemovalRule]) -> None:
        for rule in rules:
            self.apply(rule)

    def apply(self, rule: RemovalRule) -> None:
        removed_count = 0
        for entity in self.store.of_type(rule.on_type):
            value = getattr(entity, rule.target_property, None)
            if value is None:
                continue

            was_list = isinstance(value, list)
            items = value if was_list else [value]
            kept = [item for item in items if not self._matches(item, rule.where)]
            removed = len(items) - len(kept)
            if removed == 0:
                continue

            if not kept:
                new_value = None
            elif len(kept) == 1:
                new_value = kept[0]
            else:
                new_value = kept
            setattr(entity, rule.target_property, new_value)
            removed_count += removed
            logger.debug(
                "Removal %s.%s: removed %d item(s) from entity %r.",
                rule.on_type, rule.target_property, removed, entity.id,
            )

        logger.info(
            "Removal %s.%s: removed %d item(s) across %s entities.",
            rule.on_type, rule.target_property, removed_count, rule.on_type,
        )

    @staticmethod
    def _matches(item, where: RemovalWhere) -> bool:
        """True if any value resolved from ``where.property`` on ``item`` matches."""
        resolved = select_values(item, where.property)
        for value in resolved:
            text = _stringify(value)
            if where.equals is not None and text == where.equals:
                return True
            if where.contains is not None and where.contains in text:
                return True
        return False


def _stringify(value) -> str:
    """String form for matching, case-preserving.

    Integer-valued floats collapse to the equivalent int string (Pydantic's smart
    union may coerce an int ``1`` to ``1.0``), so a predicate ``equals = "1"`` still
    matches. Case is preserved — matching is case-sensitive.
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)
