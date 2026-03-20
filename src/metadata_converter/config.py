from pathlib import Path
from typing import Annotated, Any, Literal, Union

from pydantic import AnyUrl, BaseModel, Field


class ExtractorConfigBase(BaseModel):
    type: str


class ExcelExtractorConfig(ExtractorConfigBase):
    type: Literal["excel"]
    sheet_name: str
    header: int | None = None
    skiprows: list[int] | None = None
    na_values: str | None = None


class CSVExtractorConfig(ExtractorConfigBase):
    type: Literal["csv"]
    skipinitialspace: bool | None = None


ExtractorConfig = Annotated[
    Union[ExcelExtractorConfig, CSVExtractorConfig], Field(discriminator="type")
]


class CleaningConfig(BaseModel):
    strip_header_whitespace: bool = True
    normalize_empty_to_nan: bool = True
    placeholders_to_nan: bool = True
    placeholder_pattern: str = r"^.*\[.*\]$"
    strip_cell_whitespace: bool = True
    drop_fully_empty_rows: bool = True
    empty_sentinels: list[str] = Field(default_factory=lambda: ["", "N/A", "n/a", "-"])


class InputConfig(BaseModel):
    file_path: Path
    exclude_headers: list[str] | None = None
    extractor: ExtractorConfig
    cleaning: CleaningConfig


class OutputConfig(BaseModel):
    output_path: Path


class SchemaConfig(BaseModel):
    schema_url: AnyUrl | None = None
    force_update: bool | None = None
    allow_extra_fields: bool | None = None
    strict: bool | None = None


class Config(BaseModel):
    input: InputConfig
    schema_config: SchemaConfig
    output: OutputConfig
    mapping: dict[str, dict[str, Any]]
