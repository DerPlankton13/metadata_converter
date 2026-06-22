import pandas as pd

from metadata_converter.flat_data.transform.cleaning_plugin import Plugin


class CombineMonthYearRows(Plugin):
    """Combine paired month/year rows into a single row with an ISO 8601 date string.

    Detects year rows by checking if all columns except the date column are
    empty and the date column contains a plausible year value (1000-2500),
    then merges the year value into the preceding month row as ``YYYY-MM``,
    and drops the year row.

    Applies to every sheet that has a ``"Month/ Year of publication"`` column;
    other sheets are left unchanged.
    """

    DATE_COL = "Month/ Year of publication"

    def run(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        for name, df in data.items():
            if self.DATE_COL in df.columns:
                data[name] = self._combine(df)
        return data

    def _combine(self, df: pd.DataFrame) -> pd.DataFrame:
        other_cols_empty = df.drop(columns=self.DATE_COL).isna().all(axis=1)
        is_plausible_year = pd.to_numeric(df[self.DATE_COL], errors="coerce").between(
            1000, 2500
        )
        is_year_row = other_cols_empty & is_plausible_year

        for idx in df.index[is_year_row]:
            prev_idx = df.index[df.index.get_loc(idx) - 1]
            try:
                df.at[prev_idx, self.DATE_COL] = (
                    f"{df.at[idx, self.DATE_COL]}-{df.at[prev_idx, self.DATE_COL]:02d}"
                )
            except ValueError as e:
                print("Could not convert data at idx ", idx)
                print(e)

        return df.drop(index=df.index[is_year_row]).reset_index(drop=True)
