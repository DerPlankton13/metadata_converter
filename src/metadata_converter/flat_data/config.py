from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from metadata_converter.config_shared import ExcelExtractorConfig
from metadata_converter.flat_data.transform.cleaning_plugin import (
    Plugin,
    load_plugins,
)


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


FlatDataConfig.model_rebuild()
