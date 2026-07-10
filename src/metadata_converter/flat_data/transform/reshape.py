"""Reshape wide-format DataFrames into long format and split multi-value fields."""

import logging

import pandas as pd

from metadata_converter.flat_data.config import FlatDataConfig

logger = logging.getLogger(__name__)


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

    Normalises fields where values were concatenated with varied delimiters
    (commas, semicolons, ampersands, 'and'), so each value can be treated
    as a first-class row for filtering or aggregation.
    """
    field_df = df[df.header == field_to_split].copy()
    non_field_df = df[df.header != field_to_split]
    field_df.value = field_df.value.str.split(r"\s*[,;&]\s*|\s+and\s+")
    exploded_df = field_df.explode("value")
    return pd.concat([non_field_df, exploded_df], ignore_index=True)
