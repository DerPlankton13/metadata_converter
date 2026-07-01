from __future__ import annotations

import logging
import tomllib
from pathlib import Path
from typing import Annotated, Any, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from metadata_converter.api_fetching.query_models import Query
from metadata_converter.flat_data.transform.cleaning_plugin import (
    Plugin,
    load_plugins,
)

# ---------------------------------------------------------------------------
# flat_data
# ---------------------------------------------------------------------------


class FlatDataConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["flat_data"] = "flat_data"
    extractor: ExcelExtractorConfig
    cleaning: CleaningConfig
    output_dir: Path
    provenance_dir: Path | None = None
    sheet_type_mapping: dict[str, str] | None = None
    mapping: dict[str, dict[str, Any]]
    combined_columns: dict[str, dict[str, list[str]]] = Field(
        default_factory=dict,
        description=(
            "Per-sheet column combinations applied before schema building. "
            "Maps sheet name → {new_col: [source_cols]}. Source columns are joined with a space."
        ),
    )
    split_fields: dict[str, list[str]] = Field(default_factory=dict)
    broadcast_id_refs: list[BroadcastIdRef] = Field(default_factory=list)


class FlatDataUpliftConfig(BaseModel):
    """Uplift config for flat-data sources, driven by declarative rules."""

    model_config = ConfigDict(extra="forbid")
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
    removals: list[RemovalRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _no_target_overlap(self) -> FlatDataUpliftConfig:
        """Reject configs that have two rules targeting the same on_type.target_property.

        Each ``(on_type, target_property)`` may be touched by at most one rule across
        ``links``, ``enrichments`` and ``additions`` combined. The pair is the
        contract for what gets written; overlap would mean the last rule silently
        overwrites the others. ``removals`` are exempt — they legitimately undo or
        refine what another rule (or load) produced, and two removals may target
        the same list.
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


class ExcelExtractorConfig(BaseModel):
    # Also the shared base for BiosamplesExtractorConfig. If flat_data ever needs
    # an extractor-only field, pull the shared fields into a dedicated
    # ExcelReaderConfig base rather than adding it here (it would leak to biosamples).
    model_config = ConfigDict(extra="forbid")
    input: Path = Field(description="An .xlsx file, or a directory of them.")
    sheet_name: str | list[str]
    header: int | None = None
    skiprows: list[int] | None = None


class CleaningConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    strip_header_whitespace: bool = True
    strip_cell_whitespace: bool = True
    sentinels_to_na: bool = True
    empty_sentinels: list[str] = Field(default_factory=lambda: ["", "N/A", "n/a", "-"])
    placeholders_to_na: bool = True
    placeholder_pattern: str = r"^.*\[.*\]$"
    plugin_dir: Path | None = None
    plugin_name: str | list[str] | None = None
    plugins: list[Plugin] = Field(default_factory=list)

    @model_validator(mode="after")
    def load_plugins_from_dir(self) -> CleaningConfig:
        if self.plugin_dir is not None:
            self.plugins = load_plugins(self.plugin_dir, self.plugin_name)
        return self


class BroadcastIdRef(BaseModel):
    """Load-time broadcast @id reference: inject typed entity refs from one sheet into another."""

    model_config = ConfigDict(extra="forbid")
    on_sheet: str = Field(
        description="Target sheet whose entities receive the reference."
    )
    property: str = Field(description="Property to set on each target entity.")
    from_sheet: str = Field(
        description=(
            "Source sheet supplying the referenced entities. The @type of the "
            "injected references is taken from mapping[from_sheet].type."
        ),
    )
    filter_column: str | None = Field(
        None,
        description="Source column to filter on. Omit to include all entities from from_sheet.",
    )
    filter_value: Any = Field(
        None, description="Value to match (normalized string comparison)."
    )


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
            raise ValueError("exactly one of 'equals' or 'contains' must be set in `where`")
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


# ---------------------------------------------------------------------------
# biosamples
# ---------------------------------------------------------------------------


class BiosamplesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["biosamples"] = "biosamples"
    extractor: BiosamplesExtractorConfig
    fetched_dir: Path
    output_dir: Path
    provenance_dir: Path | None = None
    max_workers: int = 10
    user_agent: str = "metadata-collector/1.0"


class BiosamplesExtractorConfig(ExcelExtractorConfig):
    """The Excel reader that yields the sample IDs to fetch (a specialized
    ExcelExtractorConfig: single sheet, plus the id-column name)."""

    sheet_name: str = "sample"
    sample_id_column: str = "sample:pid"


# ---------------------------------------------------------------------------
# api
# ---------------------------------------------------------------------------


class ApiFetchingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["api"] = "api"
    extractor: ApiExtractorConfig
    fetched_dir: Path
    output_dir: Path
    provenance_dir: Path | None = None


class ApiExtractorConfig(BaseModel):
    """Configuration for a single `query_source` or `fetch_jsonld` call."""

    model_config = ConfigDict(extra="forbid")

    api_url: str = Field(description="HTTPS URL of the repository search API endpoint.")
    query: Query = Field(description="A QueryTerm or QueryGroup describing the search.")
    fetch_strategy: Literal["export_endpoint", "html_jsonld"] = Field(
        description=(
            '"export_endpoint" performs a GET to export_url_template with {record_id} substituted; '
            '"html_jsonld" fetches the landing page and extracts the first '
            '<script type="application/ld+json"> block.'
        )
    )
    export_url_template: str = Field(
        "https://zenodo.org/records/{record_id}/export/json-ld",
        description="Must contain {record_id}. Ignored when fetch_strategy='html_jsonld'.",
    )
    page_size: int = Field(
        25,
        description="Records per API page. 25 is safe for unauthenticated Zenodo requests.",
    )
    request_delay: float = Field(
        0.5, description="Seconds to sleep between paginated requests."
    )
    user_agent: str = Field(
        "metadata-collector/1.0",
        description="Value of the User-Agent header sent with every request.",
    )
    max_redirects: int = Field(
        0,
        description=(
            "Maximum HTTP redirects to follow. Default 0 (strict — most repository APIs respond directly). "
            "Raise to ~5 only when the source uses DOI resolvers or 302-based landing-page redirects. "
            "This is meant as a safety feature."
        ),
    )
    response_timeout: int = Field(120, description="HTTP response timeout in seconds.")
    max_response_mb: float = Field(
        10.0,
        description="Maximum response body in MB. Exceeding this raises ValueError. This is meant as a safety feature.",
    )

    @field_validator("api_url", "export_url_template", mode="before")
    @classmethod
    def _require_https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError(f"URL must use HTTPS: {v!r}")
        return v

    @model_validator(mode="after")
    def _export_template_has_placeholder(self) -> ApiExtractorConfig:
        if (
            self.fetch_strategy == "export_endpoint"
            and "{record_id}" not in self.export_url_template
        ):
            raise ValueError(
                "export_url_template must contain {record_id} "
                "when fetch_strategy='export_endpoint'."
            )
        return self


# ---------------------------------------------------------------------------
# uplifting
# ---------------------------------------------------------------------------


class UpliftingConfig(BaseModel):
    """Config for the uplift phase. Loaded separately from source configs — no source_type needed."""

    model_config = ConfigDict(extra="forbid")
    biosamples: SourcePaths | None = None
    api_fetching: SourcePaths | None = None
    flat_data: FlatDataUpliftConfig | None = None


class SourcePaths(BaseModel):
    """Input/output paths for one source in the uplift config."""

    model_config = ConfigDict(extra="forbid")
    input_dir: Path
    output_dir: Path
    provenance_dir: Path | None = None


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

# Discriminated union of the three data-source config types.
# Used by load_source_config for the fetch and load phases.
SourceConfig = Annotated[
    Union[FlatDataConfig, ApiFetchingConfig, BiosamplesConfig],
    Field(discriminator="source_type"),
]

source_config_adapter: TypeAdapter[SourceConfig] = TypeAdapter(SourceConfig)

logger = logging.getLogger(__name__)


def load_toml(path: str) -> dict:
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        logger.error("Config file not found: %s", path)
        raise SystemExit(1)


def handle_validation_error(e: ValidationError) -> None:
    first = e.errors()[0]
    logger.error(
        "Invalid config — %s at %s (input was: %s)",
        first["msg"],
        first["loc"],
        first["input"],
    )
    raise SystemExit(1)


def load_source_config(
    path: str,
) -> FlatDataConfig | ApiFetchingConfig | BiosamplesConfig:
    """Load and validate a source config (flat_data, biosamples, or api) from a TOML file."""
    raw = load_toml(path)
    try:
        return source_config_adapter.validate_python(raw)
    except ValidationError as e:
        handle_validation_error(e)


def load_uplift_config(path: str) -> UpliftingConfig:
    """Load and validate an uplift config from a TOML file."""
    raw = load_toml(path)
    try:
        return UpliftingConfig.model_validate(raw)
    except ValidationError as e:
        handle_validation_error(e)


# Resolve forward references introduced by the top-down ordering.
FlatDataConfig.model_rebuild()
FlatDataUpliftConfig.model_rebuild()
BiosamplesConfig.model_rebuild()
ApiFetchingConfig.model_rebuild()
UpliftingConfig.model_rebuild()
