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


class InputConfig(BaseModel):
    file_path: Path
    exclude_headers: list[str] | None = None
    extractor: ExtractorConfig


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
