import logging
import sys
from pathlib import Path

_LOG_FILE_NAME = "metadata-converter.log"


def setup_logging(level: int = logging.INFO, output_path: Path | None = None) -> None:
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(level)
    stderr_handler.setFormatter(fmt)
    root.addHandler(stderr_handler)

    if output_path is not None:
        output_path.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(output_path / _LOG_FILE_NAME, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
