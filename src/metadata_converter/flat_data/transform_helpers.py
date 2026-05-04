import pandas as pd


def combine_columns(
    df: pd.DataFrame, col1: str, col2: str, header_name: str
) -> pd.Series:
    """
    Concatenate two header values into a single Series row.

    Parameters
    ----------
    df : pd.DataFrame
        Group slice from a long-format DataFrame with 'header' and 'value' columns.
    col1 : str
        Header name of the leading value (e.g. first name).
    col2 : str
        Header name of the trailing value (e.g. last name).
    header_name : str
        Header label assigned to the combined result row.

    Returns
    -------
    pd.Series
        A single row with 'header' and 'value' keys, ready to be stacked
        back into the long-format DataFrame.
    """
    first = df.loc[df.header == col1, "value"].values[0]
    last = df.loc[df.header == col2, "value"].values[0]
    return pd.Series({"header": header_name, "value": f"{first} {last}"})


def create_full_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Append combined full-name rows to a long-format DataFrame.

    Avoids dropping the original first/last name rows so downstream code
    can still filter on either the parts or the whole.

    Parameters
    ----------
    df : pd.DataFrame
        Long-format DataFrame with at least columns 'id', 'header', 'value',
        containing 'author:first-name' and 'author:last-name' rows.

    Returns
    -------
    pd.DataFrame
        Original DataFrame extended with one 'name' row per author id.
    """
    full_names = (
        df.groupby("id")
        .apply(combine_columns, "author:first-name", "author:last-name", "name")
        .reset_index()
    )
    return pd.concat([df, full_names], ignore_index=True)


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
