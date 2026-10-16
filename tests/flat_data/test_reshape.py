"""Tests for split_field.

The frame under test is the long format ``convert_to_long`` produces: one row per
(entity, column), with ``id`` naming the entity, ``header`` the column its value
came from, and ``value`` the cell. Each test asserts the complete output row —
``[id, header, value]`` — because carrying the source row's ``id`` and ``header``
onto every value is part of what splitting a field means, not a detail of how it
is done.
"""

import pandas as pd
import pytest

from metadata_converter.flat_data.transform.reshape import split_field

# ---------------------------------------------------------------------------
# split_field — delimiter recognition
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cell",
    [
        pytest.param("a,b", id="comma-no-space"),
        pytest.param("a ; b", id="semicolon-spaced"),
        pytest.param("a&b", id="ampersand-no-space"),
        pytest.param("a \t\n , \t b", id="tabs-and-newlines"),
    ],
)
def test_split_punctuation_delimiter_yields_one_row_per_value(cell):
    df = pd.DataFrame({"id": ["0"], "header": ["analysis:keywords"], "value": [cell]})

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == [
        ["0", "analysis:keywords", "a"],
        ["0", "analysis:keywords", "b"],
    ]


def test_split_and_only_when_whitespace_delimited():
    df = pd.DataFrame(
        {
            "id": ["0", "1"],
            "header": ["analysis:keywords", "analysis:keywords"],
            "value": ["a and b", "Ireland & sandbox"],
        }
    )

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == [
        ["0", "analysis:keywords", "a"],
        ["0", "analysis:keywords", "b"],
        ["1", "analysis:keywords", "Ireland"],
        ["1", "analysis:keywords", "sandbox"],
    ]


@pytest.mark.parametrize(
    "cell",
    [
        pytest.param("a And b", id="title-case"),
        pytest.param("a AND b", id="upper-case"),
        pytest.param("a aNd b", id="mixed-case"),
    ],
)
def test_split_and_matches_any_case(cell):
    df = pd.DataFrame({"id": ["0"], "header": ["analysis:keywords"], "value": [cell]})

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == [
        ["0", "analysis:keywords", "a"],
        ["0", "analysis:keywords", "b"],
    ]


def test_split_delimiter_absorbs_following_and():
    df = pd.DataFrame(
        {"id": ["0"], "header": ["analysis:keywords"], "value": ["x, y, and z"]}
    )

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == [
        ["0", "analysis:keywords", "x"],
        ["0", "analysis:keywords", "y"],
        ["0", "analysis:keywords", "z"],
    ]


def test_split_word_starting_with_and_not_absorbed():
    df = pd.DataFrame(
        {"id": ["0"], "header": ["analysis:keywords"], "value": ["a, android b"]}
    )

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == [
        ["0", "analysis:keywords", "a"],
        ["0", "analysis:keywords", "android b"],
    ]


def test_split_value_without_delimiter_yields_single_row():
    df = pd.DataFrame({"id": ["0"], "header": ["analysis:keywords"], "value": ["solo"]})

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == [
        ["0", "analysis:keywords", "solo"]
    ]


# ---------------------------------------------------------------------------
# split_field — degenerate and non-string input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cell, expected",
    [
        pytest.param(
            "a,,b",
            [["0", "analysis:keywords", "a"], ["0", "analysis:keywords", "b"]],
            id="adjacent-delimiters",
        ),
        pytest.param(",a", [["0", "analysis:keywords", "a"]], id="leading-delimiter"),
        pytest.param("a,", [["0", "analysis:keywords", "a"]], id="trailing-delimiter"),
        pytest.param(",", [], id="delimiter-only"),
    ],
)
def test_split_degenerate_delimiters_yield_no_empty_values(cell, expected):
    df = pd.DataFrame({"id": ["0"], "header": ["analysis:keywords"], "value": [cell]})

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == expected


def test_split_non_string_value_kept_as_string():
    df = pd.DataFrame({"id": ["0"], "header": ["analysis:keywords"], "value": [12345]})

    out = split_field(df, "analysis:keywords")

    assert out[["id", "header", "value"]].to_numpy().tolist() == [
        ["0", "analysis:keywords", "12345"]
    ]


def test_split_missing_value_preserved():
    df = pd.DataFrame({"id": ["0"], "header": ["analysis:keywords"], "value": [pd.NA]})

    out = split_field(df, "analysis:keywords")

    # the missing value cannot be compared by equality, so the row is asserted in
    # two parts: the columns that carry over, then the value that must stay missing
    assert out[["id", "header"]].to_numpy().tolist() == [["0", "analysis:keywords"]]
    assert pd.isna(out.value.iloc[0])


# ---------------------------------------------------------------------------
# split_field — row selection
# ---------------------------------------------------------------------------


def test_split_other_headers_untouched():
    df = pd.DataFrame(
        {
            "id": ["0", "1"],
            "header": ["analysis:keywords", "analysis:name"],
            "value": ["a,b", "c,d"],
        }
    )

    out = split_field(df, "analysis:keywords")

    # filtered by header rather than asserted as one list: which header comes back
    # first is incidental, so pinning a whole-frame order would freeze a detail
    # this function does not promise
    assert out[out.header == "analysis:name"][
        ["id", "header", "value"]
    ].to_numpy().tolist() == [
        ["1", "analysis:name", "c,d"],
    ]
    assert out[out.header == "analysis:keywords"][
        ["id", "header", "value"]
    ].to_numpy().tolist() == [
        ["0", "analysis:keywords", "a"],
        ["0", "analysis:keywords", "b"],
    ]
