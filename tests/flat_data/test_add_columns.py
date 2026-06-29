"""Tests for add_combined_columns and add_id."""
import pandas as pd
import pytest

from metadata_converter.flat_data.transform.add_columns import add_combined_columns, add_id


# ---------------------------------------------------------------------------
# add_combined_columns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_data, combines, expected_col, expected_values",
    [
        pytest.param(
            {"first": ["Ada"], "last": ["Lovelace"]},
            {"full_name": ["first", "last"]},
            "full_name", ["Ada Lovelace"],
            id="two-columns",
        ),
        pytest.param(
            {"a": ["x"], "b": ["y"], "c": ["z"]},
            {"abc": ["a", "b", "c"]},
            "abc", ["x y z"],
            id="three-columns",
        ),
    ],
)
def test_add_combined_columns_joins_with_space(source_data, combines, expected_col, expected_values):
    df = pd.DataFrame(source_data)
    df = add_combined_columns(df, combines)
    assert df[expected_col].tolist() == expected_values


def test_add_combined_columns_empty_adds_no_columns():
    df = pd.DataFrame({"col": ["v"]})
    assert list(add_combined_columns(df, {}).columns) == ["col"]


def test_add_combined_columns_skips_na_value():
    df = pd.DataFrame({"first": ["Ada", pd.NA], "last": ["Lovelace", "Smith"]})
    df = add_combined_columns(df, {"full": ["first", "last"]})
    assert df["full"].iloc[0] == "Ada Lovelace"
    assert df["full"].iloc[1] == "Smith"


def test_add_combined_columns_both_na_produces_na():
    df = pd.DataFrame({"first": [pd.NA], "last": [pd.NA]})
    df = add_combined_columns(df, {"full": ["first", "last"]})
    assert pd.isna(df["full"].iloc[0])


def test_add_combined_columns_multiple_targets():
    df = pd.DataFrame({"a": ["x"], "b": ["y"], "c": ["z"]})
    df = add_combined_columns(df, {"ab": ["a", "b"], "bc": ["b", "c"]})
    assert df["ab"].tolist() == ["x y"]
    assert df["bc"].tolist() == ["y z"]


# ---------------------------------------------------------------------------
# add_id
# ---------------------------------------------------------------------------


def test_add_id_returns_typed_prefix_and_jsonld_suffix():
    df = pd.DataFrame({"foo": ["x"]})
    out = add_id(df, "Person")
    id_value = out["@id"][0]
    assert id_value[:7] == "Person_"
    assert id_value[-7:] == ".jsonld"


def test_add_id_different_rows_get_different_ids():
    df = pd.DataFrame({"foo": ["x", "y"]})
    out = add_id(df, "Person")
    assert out["@id"][0] != out["@id"][1]


def test_add_id_same_content_gets_same_id():
    df = pd.DataFrame({"foo": ["x", "x"]})
    out = add_id(df, "Person")
    assert out["@id"][0] == out["@id"][1]


def test_add_id_is_deterministic():
    df = pd.DataFrame({"foo": ["x"]})
    id1 = add_id(df.copy(), "Person")["@id"][0]
    id2 = add_id(df.copy(), "Person")["@id"][0]
    assert id1 == id2
