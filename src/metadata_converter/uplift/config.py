from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Condition(BaseModel):
    """Test one property of the value a rule would replace before the rule applies.

    ``if_property`` is read from that value (e.g. the ``propertyID`` of an existing
    PropertyValue) and compared as text with ``equals``. A plain string has no
    properties, so it never meets a condition.
    """

    model_config = ConfigDict(extra="forbid")
    if_property: str = Field(
        description="The Property which will be tested against 'equals' to determine"
        " whether the condition is true."
    )
    equals: str = Field(
        description="The string value against which to test the condition."
    )


class EnrichmentRule(BaseModel):
    """Wrap a scalar property value in a custom PropertyValue subclass at uplift time.

    For each entity of ``on_type``, the applier reads ``target_property`` and replaces
    the scalar value with ``cls(value=scalar)`` where ``cls`` is resolved from
    ``enrich_as``. The class's Pydantic validators populate the rest of the enriched
    PropertyValue (url, name, propertyID, etc.). With a ``condition``, only values whose
    ``condition.if_property`` equals ``condition.equals`` are enriched.
    """

    model_config = ConfigDict(extra="forbid")
    on_type: str = Field(description="@type of entities to modify.")
    target_property: str = Field(
        description="Property whose scalar value will be wrapped."
    )
    enrich_as: str = Field(
        description="Class name to construct around the scalar (e.g. 'Orcid', 'DOI'). "
        "Must name a PropertyValue subclass. The scalar becomes the class's "
        "``value`` field; the class's validators fill out the rest."
    )
    condition: Condition | None = Field(
        description="Optional: only values meeting this condition are enriched; all "
        "others are left as loaded.",
        default=None,
    )


class AdditionRule(BaseModel):
    """Set a property to a fixed constant value on every entity of a type at uplift time.

    The ``value`` is either a *literal* (a scalar DataType — Text/Number/Boolean) set
    directly, a *node* (a mapping carrying a ``type`` key, plus ``id`` and any schema.org
    fields, that builds a typed schema object, recursively), or a list of these (set
    as-is, not collapsed). The constant overwrites any existing value of ``target_property``.
    """

    model_config = ConfigDict(extra="forbid")
    on_type: str = Field(description="@type of entities to modify.")
    target_property: str = Field(description="Property to set on each on_type entity.")
    value: str | int | float | bool | list[Any] | dict[str, Any] = Field(
        description="The constant to set: a literal, a node (mapping with a 'type' key), or a list of these."
    )


class GenericUpliftConfig(BaseModel):
    """Source-independent uplift config, driven entirely by declarative rules."""

    model_config = ConfigDict(extra="forbid")
    input_dir: Path = Field(
        description="The single directory of loaded JSON-LD to uplift. Deliberately not "
        "a list: everything loaded here is written back to output_dir and recorded in "
        "provenance_dir, so naming a sibling source would copy it into this source's "
        "output and attribute its entities to themselves."
    )
    output_dir: Path
    provenance_dir: Path
    enrichments: list[EnrichmentRule] = Field(default_factory=list)
    additions: list[AdditionRule] = Field(default_factory=list)
    atomize: bool = Field(
        default=True,
        description="Extract every blank node reachable from any entity into its own "
        "standalone, content-hashed entity, replacing it with a reference. Runs last, "
        "after all rule-driven operations above.",
    )

    @model_validator(mode="after")
    def _no_target_overlap(self) -> GenericUpliftConfig:
        """Reject configs that have two rules targeting the same on_type.target_property.

        Each ``(on_type, target_property)`` may be touched by at most one rule across
        ``enrichments`` and ``additions`` combined. The pair is the contract for
        what gets written; overlap would mean the last rule silently overwrites the
        others.

        The one exception is enrichments that each carry a ``condition`` on the same
        ``if_property`` with different ``equals`` values: a property has one value, so
        at most one of them can match. Conditions on different properties are still
        rejected, because both could match the same entity.
        """
        seen: dict[tuple[str, str], list[tuple[str, Condition | None]]] = {}
        rules_by_kind = (
            *(("enrichment", r) for r in self.enrichments),
            *(("addition", r) for r in self.additions),
        )
        for kind, rule in rules_by_kind:
            key = (rule.on_type, rule.target_property)
            condition = getattr(rule, "condition", None)
            for existing_kind, existing in seen.get(key, []):
                exclusive = (
                    condition is not None
                    and existing is not None
                    and condition.if_property == existing.if_property
                    and condition.equals != existing.equals
                )
                if not exclusive:
                    raise ValueError(
                        f"{rule.on_type}.{rule.target_property} is targeted by multiple "
                        f"uplift rules: {existing_kind!r} rule and {kind!r} rule. "
                        f"Configure them on different target properties, consolidate, "
                        f"or give both a condition on the same property with different "
                        f"'equals' values."
                    )
            seen.setdefault(key, []).append((kind, condition))
        return self


GenericUpliftConfig.model_rebuild()


class UpliftMergeConfig(BaseModel):
    """The config for performing the merge during the uplift phase."""

    graph_input_path: Path = Field(
        description="Path to the graph file built from the atomised files.",
    )
    graph_output_path: Path = Field(
        description="Path to the graph file where duplicated entities have been merged.",
    )
    provenance_dir: Path
