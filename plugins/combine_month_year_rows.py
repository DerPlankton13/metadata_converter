import pandas as pd

from metadata_converter.cleaning_plugin import CleaningPlugin


class CombineMonthYearRows(CleaningPlugin):
    """
    Combine paired month/year rows into a single row with an ISO 8601 date string.

    Detects year rows by checking if all columns except the date column are
    empty and the date column contains a plausible year value (1000-2500),
    then merges the year value into the preceding month row as ``YYYY-MM``,
    and drops the year row.

    Parameters
    ----------
    date_col : str
        Name of the column containing the month (int) and year values.

    Examples
    --------
    >>> cleaner = CombineMonthYearRows(date_col="date")
    >>> df = pd.DataFrame({"date": [1, 2024, "foo"], "other": ["bar", None, "baz"]})
    >>> cleaner.run(df)
      date other
    0  2024-01   bar
    1      foo   baz
    """

    DATE_COL = "Month/ Year of publication"

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the cleaning step.

        Parameters
        ----------
        df : pd.DataFrame
            Input dataframe where every month row is followed by a year row
            containing only the date column filled.

        Returns
        -------
        pd.DataFrame
            Dataframe with year rows removed and the date column formatted as
            ``YYYY-MM`` strings.
        """
        other_cols_empty = df.drop(columns=self.DATE_COL).isna().all(axis=1)
        is_plausible_year = pd.to_numeric(df[self.DATE_COL], errors="coerce").between(
            1000, 2500
        )
        is_year_row = other_cols_empty & is_plausible_year

        for idx in df.index[is_year_row]:
            prev_idx = df.index[df.index.get_loc(idx) - 1]
            df.at[prev_idx, self.DATE_COL] = (
                f"{df.at[idx, self.DATE_COL]}-{df.at[prev_idx, self.DATE_COL]:02d}"
            )

        return df.drop(index=df.index[is_year_row]).reset_index(drop=True)
