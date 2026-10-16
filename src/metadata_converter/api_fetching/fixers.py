"""Source-specific repairs for known malformed shapes in fetched JSON-LD.

Fixers allow loading the fetched data to graph space by operating on the
raw dict *before* schema validation. In this manner we only have valid
schemas in loaded_base.
They correct provider bugs, but do not enrich or link data, which belongs
to the uplift stage.
"""

import logging
from typing import Callable
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


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


def fix_zenodo_at_ids(jsonld: dict) -> dict:
    """Fixes @id that point to Orcid or ROR outside the graph.

    If a node is typed and contains properties besides the @id, it is part of the
    graph. Thus, the @id should point to this entry in the graph so that using http
    will provide the information stored in this node according to the linked data
    principles. If the @id points to an external source such as Orcid or ROR instead,
    the information stored under this node cannot be fetched, even though the external
    source may describe the same entity.
    """

    def harmonize_id(subdict: dict) -> None:
        # only replace if @id, @type and at least another property are present
        if (
            "@id" in subdict
            and "@type" in subdict
            and len([k for k in subdict if not k.startswith("@")]) > 0
        ):
            node_id = subdict["@id"]
            if not isinstance(node_id, str):
                logger.error("Invalid @id encountered in Zenodo input: %s", node_id)
                return
            # only do the replacement it the id is a url pointing to orcid or ror
            if urlparse(node_id).hostname in ("orcid.org", "ror.org"):
                subdict.pop("@id")
                if (
                    subdict.get("identifier") is not None
                    and subdict["identifier"] != node_id
                ):
                    logger.warning(
                        "The zenodo record already contains an identifier %s, it is replaced by the @id %s",
                        subdict["identifier"],
                        node_id,
                    )
                subdict["identifier"] = node_id

    def harmonize_walker(item: object) -> None:
        if isinstance(item, dict):
            harmonize_id(item)
            for child in item.values():
                harmonize_walker(child)
        elif isinstance(item, list):
            for subitem in item:
                harmonize_walker(subitem)

    harmonize_walker(jsonld)

    return jsonld


FIXER = Callable[[dict], dict]
FIXERS: dict[str, FIXER] = {
    "fix_zenodo_funding_url": fix_zenodo_funding_url,
    "fix_zenodo_at_ids": fix_zenodo_at_ids,
}
