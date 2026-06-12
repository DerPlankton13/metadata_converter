import logging
from pathlib import Path

import pandas as pd

from metadata_converter.config import FlatDataConfig

logger = logging.getLogger(__name__)


def extract_data(
    config: FlatDataConfig, file_path: Path | None = None
) -> dict[str, pd.DataFrame]:
    """Extract tabular data from a single file.

    ``file_path`` overrides ``config.extractor.file_path`` so callers can loop
    over files in a directory while reusing the same config.
    """
    extractor_cfg = config.extractor
    path = file_path or extractor_cfg.file_path

    logger.info("Extracting data from %s", path)
    input_data = pd.read_excel(path, **extractor_cfg.model_dump(exclude={"file_path"}))
    # ensures that a dict is also returned, if only a single sheet was selected
    if isinstance(input_data, pd.DataFrame):
        input_data = {extractor_cfg.sheet_name: input_data}
    return input_data
