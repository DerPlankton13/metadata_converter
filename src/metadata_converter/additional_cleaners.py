import pandas as pd


def combine_month_year_rows(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Combine paired month/year rows into a single row with an ISO 8601 date string.

    Detects year rows by checking if all columns except the date column are
    empty and the date column contains a plausible year value (1000-2500),
    then merges the year value into the preceding month row as ``YYYY-MM``,
    and drops the year row.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe where every month row is followed by a year row
        containing only the date column filled.
    date_col : str
        Name of the column containing the month (int) and year values.

    Returns
    -------
    pd.DataFrame
        Dataframe with year rows removed and the date column formatted as
        ``YYYY-MM`` strings.

    Examples
    --------
    >>> df = pd.DataFrame({"date": [1, 2024, "foo"], "other": ["bar", None, "baz"]})
    >>> combine_month_year_rows(df, "date")
      date other
    0  2024-01   bar
    1      foo   baz
    """
    other_cols_empty = df.drop(columns=date_col).isna().all(axis=1)
    is_plausible_year = pd.to_numeric(df[date_col], errors="coerce").between(1000, 2500)
    is_year_row = other_cols_empty & is_plausible_year

    for idx in df.index[is_year_row]:
        prev_idx = df.index[df.index.get_loc(idx) - 1]
        df.at[prev_idx, date_col] = (
            f"{df.at[idx, date_col]}-{df.at[prev_idx, date_col]:02d}"
        )

    return df.drop(index=df.index[is_year_row]).reset_index(drop=True)
