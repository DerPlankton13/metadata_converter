import pandas as pd


def combine_columns(
    df: pd.DataFrame, col1: str, col2: str, header_name: str
) -> pd.Series:
    first = df.loc[df.header == col1, "value"].values[0]
    last = df.loc[df.header == col2, "value"].values[0]
    return pd.Series({"header": header_name, "value": f"{first} {last}"})


def create_full_names(df: pd.DataFrame) -> pd.DataFrame:
    full_names = (
        df.groupby("id")
        .apply(combine_columns, "author:first-name", "author:last-name", "name")
        .reset_index()
    )
    return pd.concat([df, full_names], ignore_index=True)


def split_field(df: pd.DataFrame, field_to_split: str) -> pd.DataFrame:

    field_df = df[df.header == field_to_split].copy()
    non_field_df = df[df.header != field_to_split]

    field_df.value = field_df.value.str.split(r"\s*[,;&]\s*|\s+and\s+")
    exploded_df = field_df.explode("value")
    df = pd.concat([non_field_df, exploded_df], ignore_index=True)
    return df
