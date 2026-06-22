import logging
from pathlib import Path

import pandas as pd

from metadata_converter.config import FlatDataConfig

logger = logging.getLogger(__name__)


def extract_data(
    config: FlatDataConfig, input: Path | None = None
) -> dict[str, pd.DataFrame]:
    """Extract tabular data from a single file.

    ``input`` overrides ``config.extractor.input`` so callers can loop
    over files in a directory while reusing the same config.
    """
    extractor_cfg = config.extractor
    path = input or extractor_cfg.input

    logger.info("Extracting data from %s", path)
    input_data = pd.read_excel(path, **extractor_cfg.model_dump(exclude={"input"}))
    # ensures that a dict is also returned, if only a single sheet was selected
    if isinstance(input_data, pd.DataFrame):
        input_data = {extractor_cfg.sheet_name: input_data}
    return input_data
