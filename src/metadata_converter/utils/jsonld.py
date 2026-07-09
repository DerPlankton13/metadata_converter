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
