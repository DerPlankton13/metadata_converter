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

from metadata_converter.flat_data.cleaning_plugin import CleaningPlugin, load_plugins
from metadata_converter.linked_data.query_models import Query


class ExtractorConfigBase(BaseModel):
    type: str
    file_path: Path


class ExcelExtractorConfig(ExtractorConfigBase):
    type: Literal["excel"]
    sheet_name: str | list[str]
    header: int | None = None
    skiprows: list[int] | None = None


class CsvExtractorConfig(ExtractorConfigBase):
    type: Literal["csv"]
    skipinitialspace: bool | None = None


TabularExtractorConfig = Annotated[
    Union[ExcelExtractorConfig, CsvExtractorConfig], Field(discriminator="type")
]


class ApiExtractorConfig(ExtractorConfigBase):
    """
    Configuration for a single `query_source` or `fetch_jsonld` call.

    All behaviour is explicit — there are no hidden defaults derived from the
    source name or URL. The appropriate query handler is selected automatically
    from ``api_url`` via `_QUERY_HANDLERS`.

    Parameters
    ----------
    api_url :
        HTTPS URL of the repository search API endpoint.
    query :
        A `QueryTerm` or `QueryGroup` describing the search.
    fetch_strategy :
        Strategy for retrieving JSON-LD for each record:

        - ``"export_endpoint"`` — performs a GET request to
          ``export_url_template`` with ``{record_id}`` substituted.
        - ``"html_jsonld"``     — fetches the record's landing page and
          extracts the first ``<script type="application/ld+json">`` block.
    export_url_template :
        URL template used by the ``"export_endpoint"`` fetch strategy.
        Must contain the literal placeholder ``{record_id}``.
        Ignored when ``fetch_strategy="html_jsonld"``.
    page_size :
        Number of records to request per API page. Defaults to ``25``,
        which is safe for unauthenticated Zenodo requests.
    request_delay :
        Seconds to sleep between paginated requests. Defaults to ``0.5``.
    user_agent :
        Value of the ``User-Agent`` HTTP header sent with every request.
    max_redirects :
        Maximum number of HTTP redirects to follow. Set to ``0`` to
        disallow redirects entirely. Defaults to ``0``.
    max_response_mb :
        Maximum acceptable response body size in megabytes. Requests
        exceeding this limit raise `ValueError`. Defaults to ``10.0``.
    """

    type: Literal["api"]
    api_url: str
    query: Query
    fetch_strategy: Literal["export_endpoint", "html_jsonld"]
    export_url_template: str = "https://zenodo.org/records/{record_id}/export/json-ld"
    page_size: int = 25
    request_delay: float = 0.5
    user_agent: str = "metadata-collector/1.0"
    max_redirects: int = 0
    response_timeout: int = 120
    max_response_mb: float = 10.0

    @field_validator("api_url", "export_url_template", mode="before")
    @classmethod
    def _require_https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError(f"URL must use HTTPS: {v!r}")
        return v

    @model_validator(mode="after")
    def _export_template_has_placeholder(self) -> "ApiExtractorConfig":
        if (
            self.fetch_strategy == "export_endpoint"
            and "{record_id}" not in self.export_url_template
        ):
            raise ValueError(
                "export_url_template must contain {record_id} "
                "when fetch_strategy='export_endpoint'."
            )
        return self


class CleaningConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
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
    def load_plugins_from_dir(self) -> "CleaningConfig":
        if self.plugin_dir is not None:
            self.plugins = load_plugins(self.plugin_dir, self.plugin_name)
        return self


class OutputConfig(BaseModel):
    output_path: Path


class FlatDataConfig(BaseModel):
    workflow_type: Literal["flat_data"] = "flat_data"
    extractor: TabularExtractorConfig
    cleaning: CleaningConfig
    output: OutputConfig
    sheet_type_mapping: dict[str, str] | None = None
    mapping: dict[str, dict[str, Any]]


class MetadataCollectorConfig(BaseModel):
    workflow_type: Literal["metadata_collector"] = "metadata_collector"
    extractor: ApiExtractorConfig
    output: OutputConfig


Config = Annotated[
    Union[FlatDataConfig, MetadataCollectorConfig],
    Field(discriminator="workflow_type"),
]


def load_config(path: str) -> Config:
    try:
        with open(path, "rb") as f:
            config_file = tomllib.load(f)
            config = TypeAdapter(Config).validate_python(config_file)
    except FileNotFoundError:
        print(f"Error: config file not found: {path}")
        raise SystemExit(1)
    except ValidationError as e:
        print(f"Error: invalid config:\n{e}")
        print(
            e.errors()[0]["msg"],
            e.errors()[0]["loc"],
            "but input was:",
            e.errors()[0]["input"],
        )
        raise SystemExit(1)
    return config
