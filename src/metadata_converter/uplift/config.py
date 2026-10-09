from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EnrichmentRule(BaseModel):
    """Wrap a scalar property value in a custom PropertyValue subclass at uplift time.

    For each entity of ``on_type``, the applier reads ``target_property`` and replaces
    the scalar value with ``cls(value=scalar)`` where ``cls`` is resolved from
    ``enrich_as``. The class's Pydantic validators populate the rest of the enriched
    PropertyValue (url, name, propertyID, etc.).
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


class RenameRule(BaseModel):
    """Move a property's value to a different name at uplift time.

    For each entity of ``on_type``, the value held at ``source_property`` is moved to
    ``target_property`` (overwriting any existing value there) and ``source_property``
    is cleared. Entities with no value at ``source_property`` are left untouched.
    """

    model_config = ConfigDict(extra="forbid")
    on_type: str = Field(description="@type of entities to modify.")
    source_property: str = Field(description="Property to read and clear.")
    target_property: str = Field(description="Property to move the value to.")


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
    renames: list[RenameRule] = Field(default_factory=list)
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
        others. ``renames`` are exempt — they legitimately repoint what another rule
        (or load) produced.
        """
        seen: dict[tuple[str, str], str] = {}
        rules_by_kind = (
            *(("enrichment", r) for r in self.enrichments),
            *(("addition", r) for r in self.additions),
        )
        for kind, rule in rules_by_kind:
            key = (rule.on_type, rule.target_property)
            if key in seen:
                existing = seen[key]
                raise ValueError(
                    f"{rule.on_type}.{rule.target_property} is targeted by multiple "
                    f"uplift rules: {existing!r} rule and {kind!r} rule. Configure "
                    f"them on different target properties or consolidate."
                )
            seen[key] = kind
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
