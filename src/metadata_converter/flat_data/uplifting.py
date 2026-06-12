"""Generic JSON-LD cross-reference linker.

Reads JSON-LD entities from an input directory, validates each as its schema.org
Pydantic model, applies a list of declarative link rules to resolve cross-references,
and writes the results through the unified ``load_to_jsonld`` export.

The engine is project-agnostic. All linking semantics live in
``FlatDataUpliftConfig.links`` rules — see ``LinkRule`` in ``config.py``.

How link rules work
-------------------
Each rule locates entities of ``in_type`` whose lookup value matches a value drawn
from the entity being processed, then writes back a reference::

    for every entity of on_type:
        value   = match_literal  OR  select_values(entity, match_value)
        matches = [c for c in in_type if candidate_value(c) == value]
        entity[target_property] = reference(s) to matches

Candidate values are read either via ``in_property`` (a dot-selector on the model)
or via ``in_additional_property`` (finds the ``additionalProperty`` item whose ``name``
equals the specified string, then reads its ``value`` field).

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

import json
import logging
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from metadata_converter.config import FlatDataUpliftConfig, LinkRule
from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.custom_models import get_schema
from metadata_converter.schema_org_models.schemaorg_models import (
    PropertyValue,
    SchemaOrgBase,
)
from metadata_converter.utils.log_setup import log_validation_error

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Selector evaluation primitives
# ---------------------------------------------------------------------------


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

    Parameters
    ----------
    obj : Any
        Root object to start walking from. May be a Pydantic model, a list of
        models, or any scalar. ``None`` produces an empty result.
    selector : str
        Dot-separated chain of field names, e.g. ``"agent.identifier"``.

    Returns
    -------
    list
        All values found at the end of the path. Empty when any segment is
        missing or ``None``.
    """
    if obj is None:
        return []
    if selector == "":
        return _unwrap_value(obj)

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
        return _unwrap_value(field_value)  # last segment reached — extract value


def _unwrap_value(value: Any) -> list:
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
            results.extend(_unwrap_value(item))
        return results
    if isinstance(value, BaseModel):
        inner = getattr(value, "value", None)
        if inner is not None:
            return _unwrap_value(inner)
        return [value]
    return [value]


def _to_lookup_key(value: Any) -> str | None:
    """Convert a raw data value to the canonical string used for candidate matching.

    Both sides of a link rule — the value read from an entity via ``match_value`` /
    ``match_literal``, and the value read from a candidate via ``in_property`` /
    ``in_additional_property`` — pass through this function before comparison.
    Using the same normalization on both sides makes matches type-independent.

    - ``None`` → ``None`` (caller skips these).
    - ``bool`` → ``"true"`` / ``"false"`` so that ``match_literal = "true"`` matches them.
    - ``float`` with an integer value (e.g. ``1.0``) → equivalent int string.
      Pydantic's smart-mode union resolution coerces ``int 1`` to ``float 1.0``
      when the target field's union prefers ``float``; this collapse lets
      ``match_literal = "1"`` still match such a value.
    - everything else → ``str(value).strip().lower()``.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip().lower()


def _render_ref_id(template: str, candidate: SchemaOrgBase) -> str | None:
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


def _find_additional_property(entity: SchemaOrgBase, name: str) -> list:
    """Return unwrapped values from ``additionalProperty`` items matching ``name``."""
    ap = entity.additionalProperty
    if ap is None:
        return []
    items = ap if isinstance(ap, list) else [ap]
    results: list = []
    for item in items:
        if isinstance(item, PropertyValue) and item.name == name:
            results.extend(_unwrap_value(item.value))
    return results


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _load_as_model(data: dict, source: str) -> SchemaOrgBase | None:
    """Instantiate the schema.org Pydantic model for one JSON-LD entity.

    Returns ``None`` (with a warning log) when the entity has no scalar ``@type``,
    an unknown ``@type``, or fails Pydantic validation; callers may skip such
    entities cleanly.

    Parameters
    ----------
    data : dict
        Raw JSON-LD dict as loaded from a file.
    source : str
        Human-readable identifier used in log messages (typically the filename).

    Returns
    -------
    SchemaOrgBase or None
        Validated Pydantic model, or ``None`` if loading or validation failed.
    """
    entity_type = data.get("@type")
    if not isinstance(entity_type, str):
        logger.warning("%s: missing or non-scalar @type; skipping", source)
        return None
    try:
        model_cls = get_schema(entity_type)
    except KeyError:
        logger.warning("%s: unknown schema.org @type %r; skipping", source, entity_type)
        return None
    try:
        return model_cls(**data)
    except ValidationError as e:
        logger.warning(
            "%s: Pydantic validation failed for @type %r", source, entity_type
        )
        log_validation_error(e, logger, level="warning")
        return None


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class LinkEngine:
    """Generic, config-driven cross-reference linker for JSON-LD entities.

    Configuration entirely determines which entities are modified and how. The
    engine itself knows nothing about project-specific @types or property names —
    rules in ``FlatDataUpliftConfig.links`` drive everything.

    Lifecycle
    ---------
    1. ``_load_entities`` — read each ``*.jsonld`` file in ``input_path``,
       validate as its schema.org Pydantic model, and group into ``by_type``.
    2. ``_apply_rule`` (once per rule) — build a value→candidate lookup, then
       find matches for each entity of ``on_type`` and set ``target_property``
       in place. All rules operate on the same model instances in ``by_type``.
    3. ``_write_entities`` — flatten ``by_type`` and export each model via
       ``load_to_jsonld``. Entities whose ``@type`` is listed in
       ``config.drop_types`` are skipped.
    """

    def __init__(self, config: FlatDataUpliftConfig) -> None:
        self.config = config
        self.input_path = Path(config.input_path)
        self.output_path = Path(config.output_path)
        # @type name → list of models of that @type
        self.by_type: dict[str, list[SchemaOrgBase]] = {}

    def run(self) -> None:
        """Run all three phases in order: load → apply rules → write."""
        logger.info("Starting flat-data uplift from %s", self.input_path)
        self._load_entities()
        for rule in self.config.links:
            self._apply_rule(rule)
        self._write_entities()
        logger.info("Flat-data uplift complete. Output: %s", self.output_path)

    # --- Phase 1 — load -----------------------------------------------------

    def _load_entities(self) -> None:
        """Read every ``*.jsonld`` file in ``input_path`` and group models into ``by_type``.

        Files that fail to load or validate are logged and skipped — the rest of
        the run continues. A warning is logged when the input directory is empty.
        """
        files = sorted(self.input_path.glob("*.jsonld"))
        if not files:
            logger.warning("No JSON-LD files found in %s", self.input_path)
        for path in files:
            with path.open() as f:
                data = json.load(f)
            model = _load_as_model(data, path.name)
            if model is not None:
                self.by_type.setdefault(model.type, []).append(model)
        total = sum(len(models) for models in self.by_type.values())
        logger.info("Loaded %d entity file(s)", total)

    # --- Phase 2 — apply rules ----------------------------------------------

    def _build_candidates_by_value(
        self, rule: LinkRule
    ) -> dict[str, list[SchemaOrgBase]]:
        """Index all candidates of ``rule.in_type`` by their normalized lookup value.

        Returns a dict mapping each canonical string (produced by ``_to_lookup_key``)
        to the list of candidate models whose property carries that value::

            {
                "0000-0001-2345-6789": [<Person model>],
                "analysis-001":        [<Product model>, <Product model>],
            }

        The property read from each candidate is determined by the rule:

        - ``in_property`` — dot-selector walked on the model
        - ``in_additional_property`` — reads the ``value`` field of the
          ``additionalProperty`` item whose ``name`` equals the specified string

        Logs a warning when no candidates are found, since a rule with an empty
        result will never produce any links.

        Parameters
        ----------
        rule : LinkRule
            The link rule whose ``in_type``, ``in_property``, and
            ``in_additional_property`` fields determine what is indexed.

        Returns
        -------
        dict[str, list[SchemaOrgBase]]
            Normalized lookup value → list of candidate models carrying that value.
        """
        candidates_by_value: dict[str, list[SchemaOrgBase]] = {}
        for candidate in self.by_type.get(rule.in_type, []):
            if rule.in_additional_property:
                values = _find_additional_property(
                    candidate, rule.in_additional_property
                )
            else:
                values = select_values(candidate, rule.in_property)
            for value in values:
                key = _to_lookup_key(value)
                if key is None:
                    continue
                candidates_by_value.setdefault(key, []).append(candidate)

        if not candidates_by_value:
            logger.warning(
                "Rule %s.%s: no candidates of @type %r found in input; rule will have no effect",
                rule.on_type,
                rule.target_property,
                rule.in_type,
            )
        return candidates_by_value

    def _apply_rule(self, rule: LinkRule) -> None:
        """Apply a single link rule to every entity of type ``rule.on_type``.

        1. Build ``candidates_by_value`` — a dict mapping each normalized property
           value to the candidate models that carry it (via ``_build_candidates_by_value``).
        2. For each entity of ``on_type``, resolve the lookup value via ``match_literal``
           or ``match_value``, find matching candidates, and assign ``target_property``.
           Because ``SchemaOrgBase`` sets ``validate_assignment=True``, Pydantic
           validates the assignment immediately; a ``ValidationError`` is caught and
           logged so a bad rule skips the entity rather than crashing the run.
        """
        candidates_by_value = self._build_candidates_by_value(rule)

        try:
            target_cls = get_schema(rule.in_type)
        except KeyError:
            logger.warning("Rule targets unknown @type %r; skipping rule", rule.in_type)
            return

        applied = 0
        for entity in self.by_type.get(rule.on_type, []):
            lookup_values = self._lookup_values_for(rule, entity)
            if not lookup_values:
                continue

            matches = self._find_unique_matches(candidates_by_value, lookup_values)
            if not matches:
                continue

            if rule.ref_id_template:
                refs = []
                for m in matches:
                    ref_id = _render_ref_id(rule.ref_id_template, m)
                    if ref_id is None:
                        logger.warning(
                            "Rule %s.%s: ref_id_template %r could not be rendered for candidate %r; skipping",
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

    @staticmethod
    def _lookup_values_for(rule: LinkRule, entity: SchemaOrgBase) -> list:
        """Compute the lookup value(s) for ``rule`` against ``entity``.

        Returns ``[match_literal]`` when the rule carries a constant, otherwise
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
        """Look up each lookup value in ``candidates_by_value`` and return matching candidates.

        Candidates without an ``@id`` are skipped — they cannot be turned into a reference.
        Deduplication by ``@id`` prevents duplicates when multiple lookup values resolve
        to the same candidate.

        Parameters
        ----------
        candidates_by_value : dict[str, list[SchemaOrgBase]]
            Index built by ``_build_candidates_by_value``.
        lookup_values : list
            Values to look up, as returned by ``_lookup_values_for``.

        Returns
        -------
        list[SchemaOrgBase]
            Matched candidates, deduplicated by ``@id``.
        """
        matches_by_id: dict[str, SchemaOrgBase] = {}
        for value in lookup_values:
            key = _to_lookup_key(value)
            if key is None:
                continue
            for candidate in candidates_by_value.get(key, []):
                if candidate.id:
                    matches_by_id.setdefault(candidate.id, candidate)
        return list(matches_by_id.values())

    # --- Phase 3 — write ----------------------------------------------------

    def _write_entities(self) -> None:
        """Export each loaded entity through the unified ``load_to_jsonld`` helper.

        Entities whose ``@type`` appears in ``config.drop_types`` are skipped —
        they were loaded only to be available as link candidates (e.g. sample
        stubs that the biosamples uplift owns canonically).
        """
        self.output_path.mkdir(parents=True, exist_ok=True)
        dropped_types = set(self.config.drop_types)
        written = 0
        skipped = 0
        for entity_type, models in self.by_type.items():
            if entity_type in dropped_types:
                skipped += len(models)
                continue
            for model in models:
                load_to_jsonld(model, self.output_path)
                written += 1
        logger.info(
            "Wrote %d uplifted file(s) to %s (skipped %d via drop_types)",
            written,
            self.output_path,
            skipped,
        )
