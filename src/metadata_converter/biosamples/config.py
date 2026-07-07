from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from metadata_converter.config_shared import ExcelExtractorConfig


class BiosamplesFetcherConfig(BaseModel):
    """How sample records are fetched from the EBI BioSamples API."""

    model_config = ConfigDict(extra="forbid")
    user_agent: str = "metadata-collector/1.0"
    max_workers: int = 10


class BiosamplesExtractorConfig(ExcelExtractorConfig):
    """The Excel reader that yields the sample IDs to fetch (a specialized
    ExcelExtractorConfig: single sheet, plus the id-column name)."""

    sheet_name: str = "sample"
    sample_id_column: str = "sample:pid"


class BiosamplesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_type: Literal["biosamples"] = "biosamples"
    extractor: BiosamplesExtractorConfig
    fetcher: BiosamplesFetcherConfig = Field(
        default_factory=lambda: BiosamplesFetcherConfig()
    )
    fetched_dir: Path
    output_dir: Path
    provenance_dir: Path | None = None


class BiosamplesUpliftConfig(BaseModel):
    """Uplift config for biosamples sources."""

    model_config = ConfigDict(extra="forbid")
    source_type: Literal["biosamples"] = "biosamples"
    input_dir: Path
    output_dir: Path
    provenance_dir: Path | None = None


BiosamplesConfig.model_rebuild()
BiosamplesUpliftConfig.model_rebuild()
