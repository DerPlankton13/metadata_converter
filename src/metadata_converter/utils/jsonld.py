from typing import Any

from metadata_converter.utils.hashing import hashed_id


def expand_curie(curie: str, context: list | dict) -> str:
    """Expand a compact IRI to its full form using the prefix mapping in a JSON-LD @context.

    Needed because a compact IRI (e.g. "biosample:SAMEA123") only resolves relative to the
    @context of the document it came from; reusing it elsewhere (e.g. in a provenance file
    that doesn't keep that @context) requires expanding it to an absolute IRI first.

    `context` may be a single prefix-mapping dict, or the list-of-entries shape JSON-LD allows
    (e.g. a bare vocab string followed by one or more prefix-mapping dicts).
    """
    prefix, sep, local = curie.partition(":")
    if not sep:
        return curie
    entries = context if isinstance(context, list) else [context]
    for entry in entries:
        if isinstance(entry, dict) and prefix in entry:
            return entry[prefix] + local
    raise ValueError(
        f"Unknown prefix '{prefix}' in CURIE '{curie}': not found in @context"
    )


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
