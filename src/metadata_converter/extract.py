import logging
from pathlib import Path
from typing import Callable

import pandas as pd

from metadata_converter.config import ExtractorConfigBase, FlatDataConfig

logger = logging.getLogger(__name__)

ExtractorFn = Callable[[Path, ExtractorConfigBase], pd.DataFrame]


def extract_csv(file_path: Path, config: ExtractorConfigBase) -> pd.DataFrame:
    return pd.read_csv(file_path, **config.model_dump(exclude={"type", "file_path"}))


def extract_excel(file_path: Path, config: ExtractorConfigBase) -> pd.DataFrame:
    return pd.read_excel(file_path, **config.model_dump(exclude={"type", "file_path"}))


EXTRACTOR_REGISTRY: dict[str, ExtractorFn] = {
    "csv": extract_csv,
    "excel": extract_excel,
}


def extract_data(config: FlatDataConfig, file_path: Path | None = None) -> dict[str, pd.DataFrame]:
    """Extract tabular data from a single file.

    ``file_path`` overrides ``config.extractor.file_path`` so callers can loop
    over files in a directory while reusing the same config.
    """
    extractor_cfg = config.extractor
    path = file_path or extractor_cfg.file_path
    logger.info("Extracting data from %s", path)
    extractor = EXTRACTOR_REGISTRY[extractor_cfg.type]
    input_data = extractor(path, extractor_cfg)
    if isinstance(input_data, pd.DataFrame):
        input_data = {extractor_cfg.sheet_name: input_data}
    return input_data
