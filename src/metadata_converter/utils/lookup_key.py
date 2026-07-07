from typing import Any


def to_lookup_key(value: Any) -> str | None:
    """Convert a raw data value to the canonical string used for value matching.

    Both sides of a comparison — the value read from one source and the value
    read from the other — pass through this function before comparison. Using
    the same normalization on both sides makes matches type-independent.

    - ``None`` → ``None`` (caller skips these).
    - ``bool`` → ``"true"`` / ``"false"`` so that ``match_literal = "true"`` matches them.
    - ``float`` with an integer value (e.g. ``1.0``) → equivalent int string.
      Pydantic's smart-mode union resolution coerces ``int 1`` to ``float 1.0``
      when the target field's union prefers ``float``; this collapse lets
      ``match_literal = "1"`` still match such a value.
    - everything else → ``str(value).strip().lower()``.

    Used by ``flat_data.transform.id_refs_broadcasting`` for inline broadcast
    filtering and by ``uplift.link.LinkApplier`` for cross-file link matching.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip().lower()
