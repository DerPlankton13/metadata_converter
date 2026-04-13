from pathlib import Path
from typing import Callable

import pandas as pd

from metadata_converter.config import Config, ExtractorConfigBase

ExtractorFn = Callable[[Path, ExtractorConfigBase], pd.DataFrame]


def extract_csv(file_path: Path, config: ExtractorConfigBase) -> pd.DataFrame:
    return pd.read_csv(file_path, **config.model_dump(exclude={"type"}))


def extract_excel(file_path: Path, config: ExtractorConfigBase) -> pd.DataFrame:
    return pd.read_excel(file_path, **config.model_dump(exclude={"type"}))


EXTRACTOR_REGISTRY: dict[str, ExtractorFn] = {
    "csv": extract_csv,
    "excel": extract_excel,
}


def extract_data(config: Config) -> dict[str, pd.DataFrame]:
    input_cfg = config.input
    extractor = EXTRACTOR_REGISTRY[input_cfg.extractor.type]
    data = extractor(input_cfg.file_path, input_cfg.extractor)
    if isinstance(data, pd.DataFrame):
        data = {"data": data}
    return data
