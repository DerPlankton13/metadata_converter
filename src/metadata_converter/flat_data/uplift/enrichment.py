"""EnrichmentApplier: wrap a scalar property value in a custom PropertyValue subclass.

For each ``EnrichmentRule``, the applier finds entities of ``rule.on_type``, reads
``rule.target_property``, and replaces its scalar value with ``cls(value=scalar)``
where ``cls`` is resolved from ``rule.enrich_as``. The class's Pydantic validators
populate the rest of the enriched PropertyValue (url, name, propertyID, etc.).

The applier is conservative: it only handles a single value per entity. Lists of
more than one entry are treated as a likely data error and raise rather than
silently fan out — an entity should not carry multiple identifiers of the same type.
"""

import logging
from typing import Any

from pydantic import ValidationError

from metadata_converter.flat_data.uplift.config import EnrichmentRule
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import PropertyValue

logger = logging.getLogger(__name__)


class EnrichmentApplier:
    """Apply ``EnrichmentRule``s to entities held by an ``EntityStore``.

    For each rule:

    1. Resolve ``enrich_as`` to a Pydantic class; reject if unknown or not a
       PropertyValue subclass.
    2. For every entity of ``on_type``:
       - ``None`` or empty list → skip.
       - Singleton list → unwrap to its element, then proceed.
       - List with more than one entry → raise.
       - Already an instance of the target class → skip (identity preserved).
       - Otherwise → wrap as ``cls(value=current)`` and reassign.
    """

    def __init__(self, store: EntityStore) -> None:
        self.store = store

    def apply_all(self, rules: list[EnrichmentRule]) -> None:
        for rule in rules:
            self.apply(rule)

    def apply(self, rule: EnrichmentRule) -> None:
        cls = self._resolve_class(rule)

        wrapped_count = 0
        skipped_no_value = 0
        skipped_already_wrapped = 0
        for entity in self.store.of_type(rule.on_type):
            prop = getattr(entity, rule.target_property, None)
            value = self._extract_single_value(rule, prop)
            if value is None:
                skipped_no_value += 1
                logger.debug(
                    "Enrichment %s.%s → %s: entity %r has no value; skipping.",
                    rule.on_type, rule.target_property, rule.enrich_as, entity.id,
                )
                continue
            if isinstance(value, cls):
                skipped_already_wrapped += 1
                logger.debug(
                    "Enrichment %s.%s → %s: entity %r already wrapped; skipping.",
                    rule.on_type, rule.target_property, rule.enrich_as, entity.id,
                )
                continue
            try:
                wrapped = cls(value=value)
            except ValidationError as e:
                logger.warning(
                    "Enrichment %s.%s → %s: could not wrap %r — %s",
                    rule.on_type, rule.target_property, rule.enrich_as, value, e,
                )
                continue
            try:
                setattr(entity, rule.target_property, wrapped)
            except ValidationError as e:
                logger.warning(
                    "Enrichment %s.%s → %s: assignment failed for %r — %s",
                    rule.on_type, rule.target_property, rule.enrich_as, entity.id, e,
                )
                continue
            wrapped_count += 1

        log = logger.warning if wrapped_count == 0 else logger.info
        log(
            "Enrichment %s.%s → %s: wrapped %d (skipped %d with no value, "
            "%d already wrapped).",
            rule.on_type, rule.target_property, rule.enrich_as,
            wrapped_count, skipped_no_value, skipped_already_wrapped,
        )

    @staticmethod
    def _resolve_class(rule: EnrichmentRule) -> type[PropertyValue]:
        try:
            cls = get_schema(rule.enrich_as)
        except KeyError:
            raise ValueError(
                f"EnrichmentRule {rule.on_type}.{rule.target_property}: "
                f"unknown class name {rule.enrich_as!r}."
            )
        if not issubclass(cls, PropertyValue):
            raise ValueError(
                f"EnrichmentRule {rule.on_type}.{rule.target_property}: "
                f"enrich_as must name a PropertyValue subclass; got "
                f"{rule.enrich_as!r} ({cls.__name__} is not a PropertyValue)."
            )
        return cls

    @staticmethod
    def _extract_single_value(rule: EnrichmentRule, value: Any) -> Any:
        """Reduce ``value`` to the single item to enrich.

        ``None`` and empty lists return ``None`` (caller treats as skip). A
        singleton list collapses to its element. A list with more than one entry
        is treated as a data error — an entity should not carry multiple
        identifiers of the same type — and raises.
        """
        if value is None:
            return None
        if isinstance(value, list):
            if len(value) > 1:
                raise ValueError(
                    f"EnrichmentRule {rule.on_type}.{rule.target_property} "
                    f"(enrich_as={rule.enrich_as!r}): a list of identifiers of the "
                    f"same type cannot be associated with one entity. Got "
                    f"{len(value)} values: {value!r}."
                )
            if len(value) == 0:
                return None
            return value[0]
        return value
