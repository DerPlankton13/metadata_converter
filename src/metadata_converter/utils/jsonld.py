from typing import Any

from boltons.iterutils import remap, research

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


def compact(jsonld: dict, base_namespace: str) -> dict:
    """Compact every string value against `base_namespace`, JSON-LD `@vocab`-style.

    Recursively strips `base_namespace` from the start of any string value in the
    document (e.g. "@type": "https://schema.org/CreativeWork" -> "CreativeWork"),
    not just `@type` - a source may emit full IRIs for other properties too.
    Needed before schema construction, since type discrimination looks up `@type`
    by bare class name in the schema registry, not by IRI. The `@context` value
    itself (and anything nested under it) is left untouched, since it legitimately
    contains `base_namespace` as an IRI, not a value to be compacted.
    """

    def remove_base_namespace(path, key, value):
        # ignore the context
        if key == "@context" or "@context" in path:
            return key, value
        if isinstance(value, str) and value.startswith(base_namespace):
            return key, value.removeprefix(base_namespace)
        return key, value

    return remap(jsonld, visit=remove_base_namespace)


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
        query=lambda path, key, value: isinstance(value, str) and is_schema_org_root(value),
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
