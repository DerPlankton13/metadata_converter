"""Tests for ExcelExtractorConfig's per-sheet header/skiprows validation and extract_data."""
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from metadata_converter.config import ExcelExtractorConfig, FlatDataConfig
from metadata_converter.flat_data.extract import extract_data


# ---------------------------------------------------------------------------
# ExcelExtractorConfig validation
# ---------------------------------------------------------------------------


def test_header_and_skiprows_both_scalar_valid():
    config = ExcelExtractorConfig(
        input=Path("dummy.xlsx"), sheet_name=["a", "b"], header=0, skiprows=[1, 2]
    )

    assert config.header == 0
    assert config.skiprows == [1, 2]


def test_header_and_skiprows_both_dict_matching_sheets_valid():
    config = ExcelExtractorConfig(
        input=Path("dummy.xlsx"),
        sheet_name=["a", "b"],
        header={"a": 0, "b": 2},
        skiprows={"a": [], "b": [0, 1]},
    )

    assert config.header == {"a": 0, "b": 2}
    assert config.skiprows == {"a": [], "b": [0, 1]}


def test_header_dict_skiprows_scalar_raises_mixing_error():
    with pytest.raises(ValidationError, match="mixing is not supported"):
        ExcelExtractorConfig(
            input=Path("dummy.xlsx"),
            sheet_name=["a", "b"],
            header={"a": 0, "b": 2},
            skiprows=[0],
        )


def test_header_dict_missing_sheet_raises_error():
    with pytest.raises(ValidationError, match=r"missing: \{'b'\}"):
        ExcelExtractorConfig(
            input=Path("dummy.xlsx"),
            sheet_name=["a", "b"],
            header={"a": 0},
            skiprows={"a": [], "b": []},
        )


def test_header_dict_extra_sheet_raises_error():
    with pytest.raises(ValidationError, match=r"unexpected: \{'b'\}"):
        ExcelExtractorConfig(
            input=Path("dummy.xlsx"),
            sheet_name=["a"],
            header={"a": 0, "b": 2},
            skiprows={"a": [], "b": []},
        )


def test_dict_mode_with_str_sheet_name_valid():
    config = ExcelExtractorConfig(
        input=Path("dummy.xlsx"), sheet_name="a", header={"a": 0}, skiprows={"a": []}
    )

    assert config.header == {"a": 0}
    assert config.skiprows == {"a": []}


# ---------------------------------------------------------------------------
# extract_data
# ---------------------------------------------------------------------------


def build_flat_data_config(tmp_path: Path, **extractor_kwargs) -> FlatDataConfig:
    return FlatDataConfig(
        extractor=ExcelExtractorConfig(**extractor_kwargs),
        cleaning={},
        output_dir=tmp_path / "output",
        mapping={},
    )


def test_extract_uniform_header_returns_all_sheets(tmp_path):
    xlsx_path = tmp_path / "input.xlsx"
    with pd.ExcelWriter(xlsx_path) as writer:
        pd.DataFrame({"col1": ["x"], "col2": ["y"]}).to_excel(
            writer, sheet_name="a", index=False
        )
        pd.DataFrame({"col1": ["p"], "col2": ["q"]}).to_excel(
            writer, sheet_name="b", index=False
        )
    config = build_flat_data_config(
        tmp_path, input=xlsx_path, sheet_name=["a", "b"], header=0, skiprows=None
    )

    result = extract_data(config)

    assert set(result) == {"a", "b"}
    assert list(result["a"].columns) == ["col1", "col2"]
    assert result["a"].values.tolist() == [["x", "y"]]
    assert list(result["b"].columns) == ["col1", "col2"]
    assert result["b"].values.tolist() == [["p", "q"]]


def test_extract_single_sheet_str_returns_dict(tmp_path):
    xlsx_path = tmp_path / "input.xlsx"
    with pd.ExcelWriter(xlsx_path) as writer:
        pd.DataFrame({"col1": ["x"], "col2": ["y"]}).to_excel(
            writer, sheet_name="a", index=False
        )
    config = build_flat_data_config(
        tmp_path, input=xlsx_path, sheet_name="a", header=0, skiprows=None
    )

    result = extract_data(config)

    assert isinstance(result, dict)
    assert set(result) == {"a"}
    assert list(result["a"].columns) == ["col1", "col2"]
    assert result["a"].values.tolist() == [["x", "y"]]


def test_extract_dict_mode_applies_per_sheet_header_and_skiprows(tmp_path):
    xlsx_path = tmp_path / "input.xlsx"
    with pd.ExcelWriter(xlsx_path) as writer:
        # sheet "a": header at row 0, one data row.
        pd.DataFrame([["col1", "col2"], ["x", "y"]]).to_excel(
            writer, sheet_name="a", index=False, header=False
        )
        # sheet "b": title row, header at row 1, units row, one data row.
        pd.DataFrame(
            [
                ["Sheet B Title", ""],
                ["col1", "col2"],
                ["m", "kg"],
                ["p", "q"],
            ]
        ).to_excel(writer, sheet_name="b", index=False, header=False)
    config = build_flat_data_config(
        tmp_path,
        input=xlsx_path,
        sheet_name=["a", "b"],
        header={"a": 0, "b": 1},
        skiprows={"a": [], "b": [2]},
    )

    result = extract_data(config)

    assert list(result["a"].columns) == ["col1", "col2"]
    assert result["a"].values.tolist() == [["x", "y"]]
    assert list(result["b"].columns) == ["col1", "col2"]
    assert result["b"].values.tolist() == [["p", "q"]]
