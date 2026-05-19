import logging
import sys
from pathlib import Path

from pydantic import ValidationError

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
        file_handler = logging.FileHandler(
            output_path / _LOG_FILE_NAME, encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)


def _log_validation_error(error, logger) -> None:
    if not isinstance(error, ValidationError):
        return
    seen: set = set()
    for e in error.errors():
        loc = e["loc"]
        msg = e["msg"]
        # Pydantic reports one error per union alternative tried; deduplicate by
        # (message, offending key) so each distinct problem appears only once.
        dedup_key = (msg, loc[-1] if loc else None)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        field = loc[0] if loc else "?"
        offending = loc[-1] if loc else "?"
        if offending != field:
            logger.error("  %s -> %s = %r: %s", field, offending, e["input"], msg)
        else:
            logger.error("  %s = %r: %s", field, e["input"], msg)
