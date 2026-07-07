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


FIXER = Callable[[dict], dict]
FIXERS: dict[str, FIXER] = {"fix_zenodo_funding_url": fix_zenodo_funding_url}
