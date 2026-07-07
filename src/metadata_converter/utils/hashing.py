"""Deterministic content hashing, used to derive stable ``@id`` values."""

import base64
import hashlib
import json


def content_hash(data: dict) -> str:
    """Return a 22-character URL-safe base64 hash of a dict's content.

    The hash is derived from the dict serialised as canonical JSON (keys
    sorted, non-standard types coerced to str). The first 22 characters of
    the base64url-encoded SHA-256 digest are returned, encoding 132 bits of
    entropy — negligible collision probability at any realistic dataset
    size.

    Deterministic by construction: the same input dict always produces the
    same hash, regardless of how many times or in what context it is
    computed. Callers use this to derive stable, content-addressed ids
    instead of random ones (see e.g. ``flat_data.transform.add_columns.add_id``
    and ``api_fetching.run.hashed_id``).

    Two things callers must handle themselves before calling this function:
    - Only top-level ``None`` values are dropped; ``None`` nested inside a
      value (e.g. ``{"author": {"name": None}}``) is kept and serialised as
      ``null``. This only affects which fields "count" toward the hash, not
      determinism — but keep it in mind if you need to normalise a nested
      structure independent of some nested optional field.
    - Only Python ``None`` is treated as missing. Callers whose values may
      use another missing-value sentinel (e.g. pandas ``NaN``/``NaT``) must
      filter those out themselves first, otherwise the sentinel gets
      serialised into the hash input.
    """
    filtered = {k: v for k, v in data.items() if v is not None}
    canonical = json.dumps(filtered, sort_keys=True, default=str)
    digest = hashlib.sha256(canonical.encode()).digest()
    return base64.urlsafe_b64encode(digest)[:22].decode()