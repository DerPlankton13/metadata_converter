"""Reshape wide-format DataFrames into long format and split multi-value fields."""

import logging

import pandas as pd

from metadata_converter.flat_data.config import FlatDataConfig

logger = logging.getLogger(__name__)

# One cell may hold several values glued together with a comma, semicolon,
# ampersand or the word "and"; this pattern matches the glue. "and" needs
# whitespace on both sides so "Ireland" stays whole, and `(?:and\s+)?` lets a
# punctuation mark swallow a following "and" ("x, y, and z" -> three values); it
# is non-capturing because re.split inserts captured text into its output.
SPLIT_PATTERN = r"(?i)\s*[,;&]\s*(?:and\s+)?|\s+and\s+"


def split_values(values: pd.Series) -> pd.Series:
    """Split each cell of a multi-value column into a list of its values.

    Missing values stay missing rather than becoming a list.
    """
    # the .str accessor needs a string column -- it raises on a numeric one and
    # yields NA per element on a mixed one, so cast first and restore the NAs
    as_text = values.astype(str).where(values.notna())
    return as_text.str.split(SPLIT_PATTERN, regex=True)


def reshape(
    data_dict: dict[str, pd.DataFrame], config: FlatDataConfig
) -> dict[str, pd.DataFrame]:
    """Convert each sheet to long format and split configured multi-value fields."""
    new_data: dict[str, pd.DataFrame] = {}
    for name, data in data_dict.items():
        data = convert_to_long(data)
        for field in config.split_fields.get(name, []):
            data = split_field(data, field)
        new_data[name] = data
    return new_data


def convert_to_long(df: pd.DataFrame, sheet_name: str = None) -> pd.DataFrame:
    """Melt wide-format DataFrame to (id, header, value) long format."""
    df["id"] = df.index.astype(str)
    if sheet_name:
        df.id = sheet_name + "_" + df.id
    return df.melt(id_vars=["id"], var_name="header")


def split_field(df: pd.DataFrame, field_to_split: str) -> pd.DataFrame:
    """Explode a multi-value field into separate rows, one value each.

    Every produced row keeps the ``id`` and ``header`` of the row its value came
    from; rows of any other header pass through untouched.
    """
    field_df = df[df.header == field_to_split].copy()
    non_field_df = df[df.header != field_to_split]
    field_df.value = split_values(field_df.value)
    exploded_df = field_df.explode("value")
    # a leading, trailing or doubled delimiter leaves a zero-length piece behind;
    # the isna arm spares a genuinely missing cell, which is absent, not empty
    keep = exploded_df.value.isna() | (exploded_df.value != "")
    return pd.concat([non_field_df, exploded_df[keep]], ignore_index=True)
