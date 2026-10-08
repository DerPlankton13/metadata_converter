"""EnrichmentApplier: wrap a scalar property value in a custom PropertyValue subclass.

For each ``EnrichmentRule``, the applier finds every node of ``rule.on_type`` - top-level
entities as well as nodes nested inside other entities - reads ``rule.target_property``,
and replaces its scalar value with ``cls(value=scalar)`` where ``cls`` is resolved from
``rule.enrich_as``. The class's Pydantic validators populate the rest of the enriched
PropertyValue (url, name, propertyID, etc.).

The applier is conservative: it only handles a single value per entity. Lists of
more than one entry are treated as a likely data error and raise rather than
silently fan out — an entity should not carry multiple identifiers of the same type.
"""

import logging
from collections.abc import Iterator
from typing import Any

from pydantic import ValidationError

from metadata_converter.uplift.config import EnrichmentRule
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import (
    PropertyValue,
    SchemaOrgBase,
)

logger = logging.getLogger(__name__)


def iter_nodes(
    item: object, owner_id: str | None = None
) -> Iterator[tuple[SchemaOrgBase, str | None]]:
    """Yield every schema.org node in ``item``, each with the id it can be named by.

    ``EntityStore.of_type`` only holds top-level entities, so this walks ``item``
    depth-first (a node comes before the nodes inside it), through every field and
    list, to find nested nodes too. Scalars and plain dicts are not descended into.

    A blank node (one without an ``@id``) cannot be named on its own. Each node is
    therefore paired with the ``@id`` of its nearest ancestor that has one, so a log
    message can say which record it sits in.

    Parameters
    ----------
    item : object
        What to walk: a node, a list that may contain nodes, or anything else
        (which yields nothing).
    owner_id : str | None
        ``@id`` of the nearest ancestor of ``item`` that has one. Leave as ``None``
        when walking a top-level entity.

    Yields
    ------
    tuple[SchemaOrgBase, str | None]
        A node and its own ``@id``, or else its nearest ancestor's. ``None`` only if
        neither the node nor any ancestor has one.
    """
    if isinstance(item, SchemaOrgBase):
        owner_id = item.id or owner_id
        yield item, owner_id
        for _, field_value in item:
            yield from iter_nodes(field_value, owner_id)
    elif isinstance(item, list):
        for element in item:
            yield from iter_nodes(element, owner_id)


class EnrichmentApplier:
    """Apply ``EnrichmentRule``s to entities held by an ``EntityStore``.

    For each rule:

    1. Resolve ``enrich_as`` to a Pydantic class; reject if unknown or not a
       PropertyValue subclass.
    2. For every node of ``on_type``, top-level or nested (see ``nodes_of_type``):
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

    def nodes_of_type(
        self, type_name: str
    ) -> list[tuple[SchemaOrgBase, str | None]]:
        """Return every node of exactly ``type_name``, top-level or nested, with its log name."""
        # a list, not a generator: apply() mutates nodes while looping over the result
        return [
            (node, owner_id)
            for entity in self.store.all_entities()
            for node, owner_id in iter_nodes(entity)
            if node.type == type_name
        ]

    def apply(self, rule: EnrichmentRule) -> None:
        cls = self._resolve_class(rule)

        wrapped_count = 0
        skipped_no_value = 0
        skipped_already_wrapped = 0
        for entity, owner_id in self.nodes_of_type(rule.on_type):
            where = entity.id or f"a blank {rule.on_type} inside {owner_id}"
            prop = getattr(entity, rule.target_property, None)
            value = self._extract_single_value(rule, prop)
            if value is None:
                skipped_no_value += 1
                logger.debug(
                    "Enrichment %s.%s → %s: %s has no value; skipping.",
                    rule.on_type, rule.target_property, rule.enrich_as, where,
                )
                continue
            if isinstance(value, cls):
                skipped_already_wrapped += 1
                logger.debug(
                    "Enrichment %s.%s → %s: %s already wrapped; skipping.",
                    rule.on_type, rule.target_property, rule.enrich_as, where,
                )
                continue
            try:
                wrapped = cls(value=value)
            except ValidationError as e:
                logger.warning(
                    "Enrichment %s.%s → %s: could not wrap %r on %s — %s",
                    rule.on_type, rule.target_property, rule.enrich_as, value, where, e,
                )
                continue
            try:
                setattr(entity, rule.target_property, wrapped)
            except ValidationError as e:
                logger.warning(
                    "Enrichment %s.%s → %s: assignment failed for %s — %s",
                    rule.on_type, rule.target_property, rule.enrich_as, where, e,
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
