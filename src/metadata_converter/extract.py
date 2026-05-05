from pathlib import Path
from typing import Callable

import pandas as pd

from metadata_converter.config import Config, ExtractorConfigBase

ExtractorFn = Callable[[Path, ExtractorConfigBase], pd.DataFrame]


def extract_csv(file_path: Path, config: ExtractorConfigBase) -> pd.DataFrame:
    return pd.read_csv(file_path, **config.model_dump(exclude={"type", "file_path"}))


def extract_excel(file_path: Path, config: ExtractorConfigBase) -> pd.DataFrame:
    return pd.read_excel(file_path, **config.model_dump(exclude={"type", "file_path"}))


EXTRACTOR_REGISTRY: dict[str, ExtractorFn] = {
    "csv": extract_csv,
    "excel": extract_excel,
}


def extract_data(config: Config) -> dict[str, pd.DataFrame]:
    extractor_cfg = config.extractor
    extractor = EXTRACTOR_REGISTRY[extractor_cfg.type]
    input_data = extractor(extractor_cfg.file_path, extractor_cfg)
    if isinstance(input_data, pd.DataFrame):
        input_data = {extractor_cfg.sheet_name: input_data}

    return input_data
