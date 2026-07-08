"""Deterministic content hashing, used to derive stable ``@id`` values."""

import base64
import hashlib
import json
import re
from typing import Any


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


def get_type(jsonld: dict) -> str:
    """Return the bare schema.org type name from a JSON-LD dict's ``@type``, or ``"Unknown"``."""
    return jsonld.get("@type", "Unknown").split("/")[-1]


def hashed_id(jsonld: dict) -> str:
    """Content-hash-based ``@id`` fallback: ``<schema_type>_<hash>.jsonld``.

    Mirrors ``flat_data.transform.add_columns.add_id`` — used when a fetched
    record's JSON-LD has no ``@id`` of its own (e.g. SEANOE's scraped landing
    pages), so records still get a deterministic filename/id.
    """
    return f"{get_type(jsonld)}_{content_hash(jsonld)}.jsonld"


def standardise_id(jsonld: dict) -> dict:
    """Replace a non-hashed `@id` with a content hash, preserving the original as `identifier`.

    Mutates and returns `jsonld` in place.
    """
    current_id = jsonld.get("@id")
    if not is_hashed_id(current_id, get_type(jsonld)):
        jsonld["@id"] = hashed_id({k: v for k, v in jsonld.items() if k != "@id"})
        if current_id is not None and not (
            in_property(jsonld.get("identifier"), current_id)
            or in_property(jsonld.get("url"), current_id)
        ):
            jsonld["identifier"] = current_id
    return jsonld


def is_hashed_id(current_id: str, schema_type: str) -> bool:
    """Check whether `current_id` already follows the `<schema_type>_<hash>.jsonld` convention."""
    if current_id is None:
        return False
    file_name = current_id.split("/")[-1].removesuffix(".jsonld")
    pattern = rf"{re.escape(schema_type)}_[A-Za-z0-9_-]{{22}}"
    return re.fullmatch(pattern, file_name) is not None


def in_property(prop: Any, value: Any) -> bool:
    """Check whether `value` equals `prop` or is contained in it when `prop` is a list."""
    if prop is None:
        return False
    if not isinstance(prop, list):
        prop = [prop]
    if value in prop:
        return True
    return False
