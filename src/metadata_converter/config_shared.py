"""Config building blocks shared across more than one source type.

Kept separate from ``metadata_converter/config.py`` so it has no dependency on any
per-source ``config.py`` module — those modules import from here (e.g.
``BiosamplesExtractorConfig`` subclasses ``ExcelExtractorConfig``), while
``metadata_converter/config.py`` imports the per-source modules to build the
discriminated unions. Putting shared bases and per-source configs in the same module
would make that a circular import.
"""
from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExcelExtractorConfig(BaseModel):
    # Also the shared base for BiosamplesExtractorConfig. If flat_data ever needs
    # an extractor-only field, pull the shared fields into a dedicated
    # ExcelReaderConfig base rather than adding it here (it would leak to biosamples).
    model_config = ConfigDict(extra="forbid")
    input: Path = Field(description="An .xlsx file, or a directory of them.")
    sheet_name: str | list[str]
    header: int | dict[str, int] | None = None
    skiprows: list[int] | dict[str, list[int]] | None = None

    @model_validator(mode="after")
    def _check_per_sheet_dicts(self) -> ExcelExtractorConfig:
        """header/skiprows must either both be per-sheet dicts or both be scalars;
        a dict must cover exactly the sheets in sheet_name (no missing or extra keys)."""
        header_is_dict = isinstance(self.header, dict)
        if header_is_dict != isinstance(self.skiprows, dict):
            raise ValueError(
                "header and skiprows must either both be per-sheet dicts or both be "
                "scalars; mixing is not supported"
            )
        if not header_is_dict:
            return self

        sheet_names = (
            {self.sheet_name} if isinstance(self.sheet_name, str) else set(self.sheet_name)
        )
        for field_name, value in (("header", self.header), ("skiprows", self.skiprows)):
            if set(value) != sheet_names:
                missing = sheet_names - set(value)
                extra = set(value) - sheet_names
                raise ValueError(
                    f"{field_name} must have exactly one entry per sheet in "
                    f"sheet_name (missing: {missing or None}, unexpected: {extra or None})"
                )
        return self
