from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RemovalWhere(BaseModel):
    """Predicate selecting which items to remove from a list-valued property.

    Reads ``property`` (a possibly nested dot-selector) on each item and compares
    on string form. Exactly one of ``equals`` (exact) or ``contains`` (substring)
    must be set; both are case-sensitive.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    property: str = Field(
        description="Dot-selector on each item to test (e.g. 'name', 'valueReference.termCode')."
    )
    equals: str | None = Field(
        default=None, description="Exact match. Mutually exclusive with contains."
    )
    contains: str | None = Field(
        default=None, description="Substring match. Mutually exclusive with equals."
    )

    @model_validator(mode="after")
    def _exactly_one_mode(self) -> RemovalWhere:
        if (self.equals is None) == (self.contains is None):
            raise ValueError(
                "exactly one of 'equals' or 'contains' must be set in `where`"
            )
        return self


class RemovalRule(BaseModel):
    """Filter items out of a list-valued property at uplift time.

    For each entity of ``on_type``, items of ``target_property`` matching ``where``
    are removed. A single (non-list) value is treated as a one-item collection; an
    emptied collection collapses to ``None``.
    """

    model_config = ConfigDict(extra="forbid")
    on_type: str = Field(description="@type of entities to modify.")
    target_property: str = Field(description="List-valued property to filter.")
    where: RemovalWhere = Field(description="Predicate selecting items to remove.")


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


class LinkRule(BaseModel):
    """Declarative cross-reference rule for the flat-data uplift engine."""

    model_config = ConfigDict(extra="forbid")
    on_type: str = Field(description="@type of entities to modify.")
    target_property: str = Field(description="Property to set on each on_type entity.")
    match_value: str | None = Field(
        None,
        description="Dot-selector on the entity to compute the lookup value. Mutually exclusive with match_literal.",
    )
    match_literal: str | None = Field(
        None,
        description="Constant lookup value applied to all on_type entities. Mutually exclusive with match_value.",
    )
    in_type: str = Field(description="@type of candidate entities to link to.")
    in_property: str | None = Field(
        None,
        description="Dot-selector on candidates to index by. Mutually exclusive with in_additional_property.",
    )
    in_additional_property: str | None = Field(
        None,
        description="Named additionalProperty entry on candidates to index by. Mutually exclusive with in_property.",
    )
    ref_id_template: str | None = Field(
        None,
        description="Template for constructing the ref @id from the matched candidate. "
        "Use {prop} placeholders for candidate property values, e.g. 'Product_{identifier}.jsonld'. "
        "When omitted the candidate's own @id is used.",
    )

    @model_validator(mode="after")
    def _check_match_and_in(self) -> LinkRule:
        if (self.match_value is None) == (self.match_literal is None):
            raise ValueError("exactly one of match_value or match_literal must be set")
        if (self.in_property is None) == (self.in_additional_property is None):
            raise ValueError(
                "exactly one of in_property or in_additional_property must be set"
            )
        return self


class FlatDataUpliftConfig(BaseModel):
    """Uplift config for flat-data sources, driven by declarative rules."""

    model_config = ConfigDict(extra="forbid")
    source_type: Literal["flat_data"] = "flat_data"
    input_dir: Path | list[Path] = Field(
        description="One input directory, or several whose JSON-LD is merged into a "
        "single store (e.g. one per loaded source). A duplicate @id across "
        "directories is an error."
    )
    output_dir: Path
    provenance_dir: Path | None = None
    links: list[LinkRule] = Field(default_factory=list)
    enrichments: list[EnrichmentRule] = Field(default_factory=list)
    additions: list[AdditionRule] = Field(default_factory=list)
    renames: list[RenameRule] = Field(default_factory=list)
    removals: list[RemovalRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _no_target_overlap(self) -> FlatDataUpliftConfig:
        """Reject configs that have two rules targeting the same on_type.target_property.

        Each ``(on_type, target_property)`` may be touched by at most one rule across
        ``links``, ``enrichments`` and ``additions`` combined. The pair is the
        contract for what gets written; overlap would mean the last rule silently
        overwrites the others. ``removals`` and ``renames`` are exempt — they
        legitimately undo, refine, or repoint what another rule (or load) produced.
        """
        seen: dict[tuple[str, str], str] = {}
        rules_by_kind = (
            *(("link", r) for r in self.links),
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


FlatDataUpliftConfig.model_rebuild()
