"""Source-specific repairs for known malformed shapes in fetched JSON-LD.

Fixers allow loading the fetched data to graph space by operating on the
raw dict *before* schema validation. In this manner we only have valid
schemas in loaded_base.
They correct provider bugs, but do not enrich or link data, which belongs
to the uplift stage.
"""

from typing import Callable


def fix_zenodo_funding_url(jsonld: dict) -> dict:
    """Fixes the malformatted funding url entry from jsonld files from zenodo."""

    def fix_url(grant: dict):
        if not isinstance(grant, dict):
            return
        url = grant.get("url")
        if isinstance(url, dict) and "identifier" in url:
            grant["url"] = url["identifier"]

    funding = jsonld.get("funding")
    if isinstance(funding, list):
        for grant in funding:
            fix_url(grant)
    elif isinstance(funding, dict):
        fix_url(funding)

    return jsonld


def fix_zenodo_schema_base_inconsistencies(jsonld: dict) -> dict:
    """Normalise Zenodo's `@context` to match the http/https and trailing slash `@type` uses.

    Zenodo emits `"@context": "http://schema.org"` (http, no trailing slash) alongside
    `@type` values on `https://schema.org/` (https, trailing slash). Left as-is,
    `remove_base_namespace` only strips a value that starts with the exact namespace
    string found in `@context`, so this http/https and slash mismatch leaves `@type`
    unstripped and breaks Pydantic's type discrimination.
    """
    context = jsonld.get("@context")
    if context == "http://schema.org":
        jsonld["@context"] = "https://schema.org/"
    return jsonld


FIXER = Callable[[dict], dict]
FIXERS: dict[str, FIXER] = {
    "fix_zenodo_funding_url": fix_zenodo_funding_url,
    "fix_zenodo_schema_base_inconsistencies": fix_zenodo_schema_base_inconsistencies,
}
