import logging
from typing import Any

from boltons.iterutils import remap, research

from metadata_converter.utils.hashing import hashed_id

logger = logging.getLogger(__name__)


def expand_curie(curie: str, context: list | dict) -> str:
    """Expand a compact IRI to its full form using the prefix mapping in a JSON-LD @context.

    `context` may be a single prefix-mapping dict, or the list-of-entries shape JSON-LD allows
    (e.g. a bare vocab string followed by one or more prefix-mapping dicts).
    """
    prefix, sep, local = curie.partition(":")
    if not sep:
        return curie
    matches = research(context, query=lambda path, key, value: key == prefix)
    if not matches:
        raise ValueError(
            f"Unknown prefix '{prefix}' in CURIE '{curie}': not found in @context"
        )
    return matches[0][1] + local


def remove_base_namespace(jsonld: dict, base_namespace: str) -> dict:
    """Removes `base_namespace` from any string value.

    Recursively strips `base_namespace` from the start of any string value in the
    document (e.g. "@type": "https://schema.org/CreativeWork" -> "CreativeWork").
    The `@context` value itself (and anything nested under it) is left untouched,
    since it legitimately contains `base_namespace` as an IRI.
    """

    def remove(path, key, value):
        # ignore the context
        if key == "@context" or "@context" in path:
            return key, value
        if isinstance(value, str) and value.startswith(base_namespace):
            return key, value.removeprefix(base_namespace)
        return key, value

    return remap(jsonld, visit=remove)


def find_schema_namespace(context: list | dict | str | None) -> str | None:
    """Find the schema.org root IRI (in any scheme/case variant) inside a JSON-LD @context.

    `context` may be a bare vocab string, a prefix-mapping dict, or the list-of-entries
    shape JSON-LD allows. Returns `None` if no schema.org root is present.
    """
    if context is None:
        return None
    if isinstance(context, str):
        return context if is_schema_org_root(context) else None
    matches = research(
        {"@context": context},
        query=lambda path, key, value: (
            isinstance(value, str) and is_schema_org_root(value)
        ),
    )
    return matches[0][1] if matches else None


def standardise_id(jsonld: dict) -> dict:
    """Replace `@id` with a content hash, preserving the original as `identifier`.

    A fetched record's `@id` is always source-native (a DOI, a URL, ...); it can
    never already be one of our own content hashes, so this always rehashes —
    there is no "already standardised" case to detect here.

    Mutates and returns `jsonld` in place.
    """
    current_id = jsonld.get("@id")
    jsonld["@id"] = hashed_id({k: v for k, v in jsonld.items() if k != "@id"})
    if current_id is not None and not (
        in_property(jsonld.get("identifier"), current_id)
        or in_property(jsonld.get("url"), current_id)
    ):
        jsonld["identifier"] = current_id
    return jsonld


def in_property(prop: Any, value: Any) -> bool:
    """Check whether `value` equals `prop` or is contained in it when `prop` is a list."""
    if prop is None:
        return False
    if not isinstance(prop, list):
        prop = [prop]
    if value in prop:
        return True
    return False


SCHEMA_ORG_DEFAULT_CONTEXT = {"@vocab": "https://schema.org/"}


def is_schema_org_root(value: str) -> bool:
    """Check whether `value` refers to schema.org's root (not a per-term IRI)."""
    lowered = value.lower()
    if "schema.org" not in lowered:
        return False
    after = lowered.split("schema.org", 1)[1]
    return after in ("", "/")


def standardise_context(jsonld: dict) -> dict:
    """Normalise `@context` to `{"@vocab": "https://schema.org/", ...}`.

    Accepts a missing context, a bare schema.org string (in any host/scheme/case
    variant), or a list of a bare schema.org string followed by one or more dicts
    of extra prefix mappings (the shape BioSamples emits). Any other shape —
    including a per-term schema.org IRI, an unrelated string, or a dict not
    already in canonical vocab form — is left untouched and logged as an error,
    since there is no safe way to infer intent from it.
    """
    current_context = jsonld.get("@context")
    if current_context is None or current_context == SCHEMA_ORG_DEFAULT_CONTEXT:
        jsonld["@context"] = dict(SCHEMA_ORG_DEFAULT_CONTEXT)
        return jsonld
    if isinstance(current_context, str) and is_schema_org_root(current_context):
        jsonld["@context"] = dict(SCHEMA_ORG_DEFAULT_CONTEXT)
        return jsonld
    if (
        isinstance(current_context, list)
        and len(current_context) >= 2
        and isinstance(current_context[0], str)
        and is_schema_org_root(current_context[0])
        and all(isinstance(entry, dict) for entry in current_context[1:])
    ):
        merged = dict(SCHEMA_ORG_DEFAULT_CONTEXT)
        for entry in current_context[1:]:
            merged.update(entry)
        jsonld["@context"] = merged
        return jsonld
    logger.error("Unsupported @context value: %r", current_context)
    return jsonld
