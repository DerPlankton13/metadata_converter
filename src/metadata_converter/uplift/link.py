"""LinkApplier: cross-entity ``@id`` linking driven by declarative ``LinkRule``s.

For each rule, indexes candidates of ``in_type`` by their lookup value, then
for every entity of ``on_type`` resolves the lookup value and sets the target
property to a reference (or list of references) to matched candidates.
"""

import logging

from pydantic import ValidationError

from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.uplift.config import LinkRule
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.uplift.select import (
    find_additional_property,
    render_ref_id,
    select_values,
)
from metadata_converter.utils.lookup_key import to_lookup_key

logger = logging.getLogger(__name__)


class LinkApplier:
    """Apply ``LinkRule``s to entities held by an ``EntityStore``.

    Configuration entirely determines which entities are modified and how. The
    applier itself knows nothing about project-specific ``@type``s or property
    names — every rule is declarative.

    For each rule:

    1. Index candidates of ``in_type`` by their normalized lookup value.
    2. For every entity of ``on_type``, resolve the lookup value (from
       ``match_value`` or ``match_literal``), find matches, and set
       ``target_property`` to a reference (or list of references).

    Pydantic's ``validate_assignment=True`` on ``SchemaOrgBase`` means a bad
    assignment is caught and logged; the offending entity is skipped rather
    than crashing the run.
    """

    def __init__(self, store: EntityStore) -> None:
        self.store = store

    def apply_all(self, rules: list[LinkRule]) -> None:
        for rule in rules:
            self.apply(rule)

    def apply(self, rule: LinkRule) -> None:
        """Apply a single link rule to every entity of type ``rule.on_type``."""
        candidates_by_value = self._build_candidates_by_value(rule)

        try:
            target_cls = get_schema(rule.in_type)
        except KeyError:
            logger.warning("Rule targets unknown @type %r; skipping rule", rule.in_type)
            return

        applied = 0
        for entity in self.store.of_type(rule.on_type):
            lookup_values = self._lookup_values_for(rule, entity)
            if not lookup_values:
                continue

            matches = self._find_unique_matches(candidates_by_value, lookup_values)
            if not matches:
                continue

            if rule.ref_id_template:
                refs = []
                for m in matches:
                    ref_id = render_ref_id(rule.ref_id_template, m)
                    if ref_id is None:
                        logger.warning(
                            "Rule %s.%s: ref_id_template %r could not be rendered "
                            "for candidate %r; skipping",
                            rule.on_type,
                            rule.target_property,
                            rule.ref_id_template,
                            m.id,
                        )
                        continue
                    refs.append(target_cls(id=ref_id))
                if not refs:
                    continue
            else:
                refs = [target_cls(id=m.id) for m in matches]

            try:
                setattr(
                    entity,
                    rule.target_property,
                    refs[0] if len(refs) == 1 else refs,
                )
            except ValidationError as e:
                logger.warning(
                    "Rule %s.%s: assignment failed for entity %r — %s",
                    rule.on_type,
                    rule.target_property,
                    entity.id,
                    e,
                )
                continue
            applied += 1

        in_key = rule.in_additional_property or rule.in_property
        logger.info(
            "Rule %s.%s ← %s.%s: applied to %d %s entit%s",
            rule.on_type,
            rule.target_property,
            rule.in_type,
            in_key,
            applied,
            rule.on_type,
            "y" if applied == 1 else "ies",
        )

    def _build_candidates_by_value(
        self, rule: LinkRule
    ) -> dict[str, list[SchemaOrgBase]]:
        """Index candidates of ``rule.in_type`` by their normalized lookup value."""
        candidates_by_value: dict[str, list[SchemaOrgBase]] = {}
        for candidate in self.store.of_type(rule.in_type):
            if rule.in_additional_property:
                values = find_additional_property(
                    candidate, rule.in_additional_property
                )
            else:
                values = select_values(candidate, rule.in_property)
            for value in values:
                key = to_lookup_key(value)
                candidates_by_value.setdefault(key, []).append(candidate)

        if not candidates_by_value:
            logger.warning(
                "Rule %s.%s: no candidates of @type %r found in input; rule will have no effect",
                rule.on_type,
                rule.target_property,
                rule.in_type,
            )
        return candidates_by_value

    @staticmethod
    def _lookup_values_for(rule: LinkRule, entity: SchemaOrgBase) -> list:
        """Compute the lookup value(s) for ``rule`` against ``entity``.

        Returns ``[match_literal]`` when the rule carries a constant; otherwise
        evaluates ``match_value`` as a selector on the entity. Always a list so
        the caller can iterate uniformly.
        """
        if rule.match_literal is not None:
            return [rule.match_literal]
        return select_values(entity, rule.match_value)

    @staticmethod
    def _find_unique_matches(
        candidates_by_value: dict[str, list[SchemaOrgBase]],
        lookup_values: list,
    ) -> list[SchemaOrgBase]:
        """Resolve each lookup value to candidates, deduplicated by ``@id``.

        Every stored entity has an ``@id`` (``EntityStore.load`` skips those without),
        so every matched candidate can be turned into a reference.
        """
        matches_by_id: dict[str, SchemaOrgBase] = {}
        for value in lookup_values:
            key = to_lookup_key(value)
            for candidate in candidates_by_value.get(key, []):
                matches_by_id.setdefault(candidate.id, candidate)
        return list(matches_by_id.values())
