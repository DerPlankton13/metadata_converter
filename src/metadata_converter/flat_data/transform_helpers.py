import pandas as pd


def split_field(df: pd.DataFrame, field_to_split: str) -> pd.DataFrame:
    """
    Explode a multi-value field into separate rows, one value each.

    Normalises fields where values were concatenated with varied delimiters
    (commas, semicolons, ampersands, 'and'), so each value can be treated
    as a first-class row for filtering or aggregation.

    Parameters
    ----------
    df : pd.DataFrame
        Long-format DataFrame with 'header' and 'value' columns.
    field_to_split : str
        Header name identifying the rows to split.

    Returns
    -------
    pd.DataFrame
        DataFrame with the target field exploded into one row per value.
    """

    field_df = df[df.header == field_to_split].copy()
    non_field_df = df[df.header != field_to_split]

    field_df.value = field_df.value.str.split(r"\s*[,;&]\s*|\s+and\s+")
    exploded_df = field_df.explode("value")
    df = pd.concat([non_field_df, exploded_df], ignore_index=True)
    return df
