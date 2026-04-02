from pathlib import Path
from typing import Annotated, Any, Literal, Union

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, model_validator

from metadata_converter.cleaning_plugin import CleaningPlugin, load_plugins


class ExtractorConfigBase(BaseModel):
    type: str


class ExcelExtractorConfig(ExtractorConfigBase):
    type: Literal["excel"]
    sheet_name: str
    header: int | None = None
    skiprows: list[int] | None = None


class CSVExtractorConfig(ExtractorConfigBase):
    type: Literal["csv"]
    skipinitialspace: bool | None = None


ExtractorConfig = Annotated[
    Union[ExcelExtractorConfig, CSVExtractorConfig], Field(discriminator="type")
]


class CleaningConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    strip_header_whitespace: bool = True
    strip_cell_whitespace: bool = True
    sentinels_to_na: bool = True
    empty_sentinels: list[str] = Field(default_factory=lambda: ["", "N/A", "n/a", "-"])
    placeholders_to_na: bool = True
    placeholder_pattern: str = r"^.*\[.*\]$"
    plugin_dir: Path | None = None
    plugins: list[CleaningPlugin] = Field(default_factory=list)

    @model_validator(mode="after")
    def load_plugins_from_dir(self) -> "CleaningConfig":
        if self.plugin_dir is not None:
            self.plugins = load_plugins(self.plugin_dir)
        return self


class InputConfig(BaseModel):
    file_path: Path
    extractor: ExtractorConfig
    cleaning: CleaningConfig


class OutputConfig(BaseModel):
    output_path: Path


class Config(BaseModel):
    input: InputConfig
    output: OutputConfig
    mapping: dict[str, dict[str, Any]]
