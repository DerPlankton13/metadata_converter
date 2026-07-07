from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from metadata_converter.api_fetching.query_models import Query


class ApiFetcherConfig(BaseModel):
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
    def _export_template_has_placeholder(self) -> ApiFetcherConfig:
        if (
            self.fetch_strategy == "export_endpoint"
            and "{record_id}" not in self.export_url_template
        ):
            raise ValueError(
                "export_url_template must contain {record_id} "
                "when fetch_strategy='export_endpoint'."
            )
        return self


class ApiFetchingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["api"] = "api"
    fetcher: ApiFetcherConfig
    fetched_dir: Path
    output_dir: Path
    provenance_dir: Path | None = None


class ApiFetchingUpliftConfig(BaseModel):
    """Uplift config for api_fetching sources."""

    model_config = ConfigDict(extra="forbid")
    source_type: Literal["api_fetching"] = "api_fetching"
    input_dir: Path
    output_dir: Path
    provenance_dir: Path | None = None


ApiFetchingConfig.model_rebuild()
ApiFetchingUpliftConfig.model_rebuild()
