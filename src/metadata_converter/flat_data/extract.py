import logging
from pathlib import Path

import pandas as pd

from metadata_converter.flat_data.config import FlatDataConfig

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
    if isinstance(extractor_cfg.header, dict):
        # header/skiprows dict keys are validated to exactly match sheet_name,
        # so header.items() alone tells us which sheets to read.
        input_data = {
            sheet: pd.read_excel(
                path, sheet_name=sheet, header=header, skiprows=extractor_cfg.skiprows[sheet]
            )
            for sheet, header in extractor_cfg.header.items()
        }
    else:
        input_data = pd.read_excel(path, **extractor_cfg.model_dump(exclude={"input"}))
        # ensures that a dict is also returned, if only a single sheet was selected
        if isinstance(input_data, pd.DataFrame):
            input_data = {extractor_cfg.sheet_name: input_data}

    # attach source file information for e.g. plugins
    for df in input_data.values():
        df.attrs["source_file"] = str(path)

    return input_data
