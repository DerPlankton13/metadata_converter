import logging
import sys

from pydantic import ValidationError


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger to write to stderr at the given level.

    Callers control log destination via shell redirection (e.g. ``2> file.log``).
    Use ``--log-level debug`` to get full DEBUG output when investigating issues.
    """
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


def log_validation_error(
    error: ValidationError,
    logger: logging.Logger,
    level: str = "error",
) -> None:
    """Log each unique field error from a Pydantic ValidationError at the given log level."""
    if not isinstance(error, ValidationError):
        return
    log = getattr(logger, level)
    seen: set = set()
    for e in error.errors():
        loc = e["loc"]
        msg = e["msg"]
        dedup_key = (msg, loc[-1] if loc else None)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        field = loc[0] if loc else "?"
        offending = loc[-1] if loc else "?"
        if offending != field:
            log("  %s -> %s = %r: %s", field, offending, e["input"], msg)
        else:
            log("  %s = %r: %s", field, e["input"], msg)
