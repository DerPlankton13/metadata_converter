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
from metadata_converter.flat_data.cleaning_plugin import CleaningPlugin, load_plugins

# ---------------------------------------------------------------------------
# flat_data
# ---------------------------------------------------------------------------


class FlatDataConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["flat_data"] = "flat_data"
    extractor: ExcelExtractorConfig
    cleaning: CleaningConfig
    output: OutputConfig
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
    cross_sheet_refs: list[CrossSheetRef] = Field(default_factory=list)


class FlatDataUpliftConfig(BaseModel):
    """Uplift config for flat-data sources, driven by declarative link rules."""

    model_config = ConfigDict(extra="forbid")
    input_path: Path
    output_path: Path
    links: list[LinkRule] = Field(default_factory=list)
    drop_types: list[str] = Field(
        default_factory=list,
        description="@types loaded for indexing but not written to output (e.g. sample stubs).",
    )


class ExcelExtractorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    file_path: Path
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
    plugins: list[CleaningPlugin] = Field(default_factory=list)

    @model_validator(mode="after")
    def load_plugins_from_dir(self) -> CleaningConfig:
        if self.plugin_dir is not None:
            self.plugins = load_plugins(self.plugin_dir, self.plugin_name)
        return self


class CrossSheetRef(BaseModel):
    """Ingest-time cross-sheet reference: inject typed entity refs from one sheet into another."""

    model_config = ConfigDict(extra="forbid")
    on_sheet: str = Field(description="Target sheet whose entities receive the reference.")
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
    filter_value: Any = Field(None, description="Value to match (normalized string comparison).")


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
            raise ValueError("exactly one of in_property or in_additional_property must be set")
        return self


# ---------------------------------------------------------------------------
# biosamples
# ---------------------------------------------------------------------------


class BiosamplesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["biosamples"] = "biosamples"
    input: BiosamplesInput
    output: FetchedOutputConfig
    max_workers: int = 10
    user_agent: str = "metadata-collector/1.0"


class BiosamplesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    input_path: Path
    sheet_name: str = "sample"
    header: int | None = None
    skiprows: list[int] | None = None
    header_name: str = "sample:pid"


# ---------------------------------------------------------------------------
# api
# ---------------------------------------------------------------------------


class ApiFetchingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["api"] = "api"
    extractor: ApiExtractorConfig
    output: FetchedOutputConfig


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
    request_delay: float = Field(0.5, description="Seconds to sleep between paginated requests.")
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
    input_path: Path
    output_path: Path


# ---------------------------------------------------------------------------
# Shared output models
# ---------------------------------------------------------------------------


class OutputConfig(BaseModel):
    """Output config for sources with no fetch phase (flat_data)."""

    model_config = ConfigDict(extra="forbid")
    ingested: Path


class FetchedOutputConfig(BaseModel):
    """Output config for sources with a fetch phase (biosamples, api)."""

    model_config = ConfigDict(extra="forbid")
    fetched: Path
    ingested: Path


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

# Discriminated union of the three data-source config types.
# Used by load_source_config for the fetch and ingest phases.
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


def load_source_config(path: str) -> FlatDataConfig | ApiFetchingConfig | BiosamplesConfig:
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