from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Union

import pandas as pd


@dataclass
class BaseExtractor(ABC):
    input: Union[str, Path]

    @abstractmethod
    def execute(self) -> pd.DataFrame:
        pass


class CSVExtractor(BaseExtractor):
    def execute(self) -> pd.DataFrame:
        return pd.read_csv(self.input, skipinitialspace=True)


class ExcelExtractor(BaseExtractor):
    def execute(self) -> pd.DataFrame:
        df = pd.read_excel(
            self.input,
            sheet_name="Researchers",
            header=0,
            skiprows=[0, 1, 3, 4],
            na_values="Please select",
        ).dropna(how="all")
        return df
