"""
Source-agnostic metadata collection library.

Provides a unified interface for querying repository APIs and retrieving
JSON-LD metadata records. All behaviour is controlled via `ApiFetcherConfig`
— no source-specific code paths are needed for supported repositories.

Supported repositories (built-in query handlers):

- Zenodo       (``zenodo.org/api``)
- DataCite     (``api.datacite.org``)
- SEANOE       (``seanoe.org/api``)
- Figshare and Figshare-based repositories (``api.figshare.com``)

Supported fetch strategies:

- ``"export_endpoint"`` — retrieves JSON-LD from a URL template containing ``{record_id}``
- ``"html_jsonld"``     — scrapes a ``<script type="application/ld+json">`` block
                          from the record's landing page

Extending to new sources
------------------------
Pagination is necessarily source-specific, as each API has its own request
format, response structure, and record schema. To add a new source, implement
a ``_query_*`` function following the existing pattern and register it in
``QUERY_HANDLERS``. No changes to the public API are required unless a new
fetch strategy type is needed.

Public API
----------
::

    query_source(config: ApiFetcherConfig) -> list[Record]
    fetch_jsonld(record: Record, config: ApiFetcherConfig) -> dict

Examples
--------
::

    from metadata_collector import (
        ApiFetcherConfig, QueryTerm, QueryGroup, query_source, fetch_jsonld,
    )

    # Query the Zenodo BIOcean5D community
    records = query_source(ApiFetcherConfig(
        api_url="https://zenodo.org/api/records",
        query=QueryTerm(field="communities", value="horizoneurope_biocean5d"),
        fetch_strategy="export_endpoint",
        export_url_template="https://zenodo.org/records/{record_id}/export/json-ld",
    ))

    # Query DataCite by award number or project name
    records = query_source(ApiFetcherConfig(
        api_url="https://api.datacite.org/dois",
        query=QueryGroup(operator="OR", terms=[
            QueryTerm(field="fundingReferences.awardNumber", value="101059915"),
            QueryTerm(field="fundingReferences.awardTitle", value="BIOcean5D"),
        ]),
        fetch_strategy="html_jsonld",
    ))

    # Fetch JSON-LD for each discovered record
    for record in records:
        jsonld = fetch_jsonld(record, config)

Requirements
------------
::

    pip install requests beautifulsoup4 pydantic
"""

import functools
import json
import logging
import time
from collections.abc import Callable

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel

from metadata_converter.api_fetching.config import ApiFetcherConfig
from metadata_converter.api_fetching.query_models import Query, QueryGroup, QueryTerm
from metadata_converter.utils.http import make_session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Record
# ---------------------------------------------------------------------------


class Record(BaseModel):
    """
    A metadata record returned by `query_source`.

    Parameters
    ----------
    doi :
        Canonical DOI in lowercase.
    title :
        Record title as returned by the source API.
    publisher :
        Publisher name as returned by the source API.
    url :
        Landing page URL of the record.
    source_id :
        Native identifier in the source system
        (e.g. Zenodo record ID, SEANOE docId).
    """

    doi: str
    title: str
    publisher: str
    url: str
    source_id: str


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def checked(fn):
    """
    Decorator that adds raise_for_status, response size check, redirect
    control, and timeout to any function returning a `requests.Response`.

    The decorated function must accept ``session``, ``url``, ``config``
    as its first three positional arguments. The decorator injects
    ``allow_redirects`` and ``timeout`` derived from ``config`` before
    passing remaining ``**kwargs`` to the wrapped function.
    """

    @functools.wraps(fn)
    def wrapper(session, url, config, **kwargs):
        response = fn(
            session,
            url,
            config,
            allow_redirects=config.max_redirects > 0,
            timeout=config.response_timeout,
            **kwargs,
        )
        response.raise_for_status()
        check_response_size(response, config)
        return response

    return wrapper


@checked
def get(
    session: requests.Session, url: str, config: ApiFetcherConfig, **kwargs
) -> requests.Response:
    """Perform a GET request."""
    return session.get(url, **kwargs)


@checked
def post(
    session: requests.Session, url: str, config: ApiFetcherConfig, **kwargs
) -> requests.Response:
    """Perform a POST request."""
    return session.post(url, **kwargs)


def check_response_size(
    response: requests.Response, config: ApiFetcherConfig
) -> None:
    """Raise `ValueError` if the response body exceeds ``max_response_mb``."""
    max_bytes = int(config.max_response_mb * 1024 * 1024)
    # Check Content-Length header first (not always present, but cheap)
    content_length = response.headers.get("Content-Length")
    if content_length and int(content_length) > max_bytes:
        raise ValueError(
            f"Response Content-Length ({content_length} bytes) exceeds "
            f"max_response_mb={config.max_response_mb}. URL: {response.url}"
        )
    # Always verify the actual downloaded size
    if len(response.content) > max_bytes:
        raise ValueError(
            f"Response size ({len(response.content)} bytes) exceeds "
            f"max_response_mb={config.max_response_mb}. URL: {response.url}"
        )


# ---------------------------------------------------------------------------
# Query serialisation
# ---------------------------------------------------------------------------


def to_es_query(query: Query) -> str:
    """
    Serialise a `QueryTerm` or `QueryGroup` to an Elasticsearch query string.

    Parameters
    ----------
    query :
        The query to serialise.

    Returns
    -------
    str
        Elasticsearch query string, e.g. ``"communities:horizoneurope_biocean5d"``
        or ``"(a:x OR b:y)"``.
    """
    match query:
        case QueryTerm(field=f, value=v):
            return f"{f}:{v}"
        case QueryGroup(operator=op, terms=terms):
            parts = [to_es_query(t) for t in terms]
            return "(" + f" {op} ".join(parts) + ")"


# ---------------------------------------------------------------------------
# Query handlers
# ---------------------------------------------------------------------------


def query_zenodo(
    config: ApiFetcherConfig, session: requests.Session
) -> list[Record]:
    """Query handler for the Zenodo REST API."""
    params: dict = {
        "q": to_es_query(config.query),
        "size": config.page_size,
        "page": 1,
        "sort": "newest",
    }
    data = get(session, config.api_url, config, params=params).json()
    total = data["hits"]["total"]
    records: list[Record] = []
    logger.info("Zenodo: %d record(s) found", total)

    def parse(hits: list[dict]) -> None:
        for hit in hits:
            rec_id = str(hit["id"])
            records.append(
                Record(
                    doi=hit.get("doi", "").lower() or None,
                    title=hit["metadata"].get("title", "(no title)"),
                    publisher="Zenodo",
                    url=f"https://zenodo.org/records/{rec_id}",
                    source_id=rec_id,
                )
            )

    parse(data["hits"]["hits"])
    while len(records) < total:
        params["page"] += 1
        logger.debug(
            "Fetching page %d (%d/%d) ...", params["page"], len(records), total
        )
        time.sleep(config.request_delay)
        parse(
            get(session, config.api_url, config, params=params).json()["hits"]["hits"]
        )

    return records


def query_datacite(
    config: ApiFetcherConfig, session: requests.Session
) -> list[Record]:
    """Query handler for the DataCite REST API."""
    params: dict = {
        "query": to_es_query(config.query),
        "page[size]": config.page_size,
        "page[number]": 1,
    }
    data = get(session, config.api_url, config, params=params).json()
    total = data["meta"]["total"]
    records: list[Record] = []
    logger.info("DataCite: %d record(s) found", total)

    def parse(items: list[dict]) -> None:
        for item in items:
            attr = item["attributes"]
            doi = attr.get("doi", "").lower() or None
            title = attr["titles"][0]["title"] if attr.get("titles") else "(no title)"
            records.append(
                Record(
                    doi=doi,
                    title=title,
                    publisher=attr.get("publisher", "Unknown"),
                    url=attr.get("url", ""),
                    source_id=doi or item["id"],
                )
            )

    parse(data["data"])
    while len(records) < total:
        params["page[number]"] += 1
        logger.debug(
            "Fetching page %d (%d/%d) ...", params["page[number]"], len(records), total
        )
        time.sleep(config.request_delay)
        parse(get(session, config.api_url, config, params=params).json()["data"])

    return records


def query_seanoe(
    config: ApiFetcherConfig, session: requests.Session
) -> list[Record]:
    """
    Query handler for the SEANOE internal search API.

    Only `QueryTerm` is supported. Known working fields are
    ``"descriptionFulltext"`` and ``"affiliations"``. Most other fields
    return HTTP 500.

    Raises
    ------
    ValueError
        If ``config.query`` is a `QueryGroup`.
    """
    if isinstance(config.query, QueryGroup):
        raise ValueError(
            "SEANOE only supports QueryTerm, not QueryGroup. "
            "Known working fields: 'descriptionFulltext', 'affiliations'."
        )

    def payload(page: int) -> dict:
        return {
            "groupedSearch": True,
            "criteriaList": [
                {
                    "field": config.query.field,
                    "values": [
                        {"code": config.query.value, "n": 0, "name": config.query.value}
                    ],
                    "types": ["QUERY_STRING"],
                    "options": ["HIGHLIGHT_RESULTS"],
                    "weight": 1,
                },
                {
                    "name": "Relevance",
                    "field": "_score",
                    "types": ["SORT_ONLY"],
                    "options": ["SORTABLE_FIELD"],
                    "weight": 1,
                    "sortPriority": 0,
                    "order": "DESC",
                    "values": [],
                },
            ],
            "pagination": {"page": page, "size": config.page_size, "isPaginated": True},
            "languageEnum": "en",
            "defaultCriteriaValues": {},
        }

    data = post(session, config.api_url, config, json=payload(1)).json()
    total = data.get("entriesCount", 0)
    records: list[Record] = []
    logger.info("SEANOE: %d record(s) found", total)

    def localized(value) -> str:
        """SEANOE returns some text fields as {lang_code: text} dicts (keyed by
        the requested "languageEnum") rather than plain strings."""
        if isinstance(value, dict):
            return value.get("en") or next(iter(value.values()), "")
        return value

    def parse(entries: list[dict]) -> None:
        for entry in entries:
            doc_id = str(entry.get("docId", ""))
            title = localized(entry.get("title", entry.get("name", "(no title)")))
            records.append(
                Record(
                    doi=f"10.17882/{doc_id}" if doc_id else None,
                    title=title,
                    publisher="SEANOE",
                    url=entry.get("url") or entry.get("absoluteUrlLandingPage", ""),
                    source_id=doc_id,
                )
            )

    parse(data.get("responseEntries", []))
    while len(records) < total:
        time.sleep(config.request_delay)
        page = len(records) // config.page_size + 1
        logger.debug("Fetching page %d (%d/%d) ...", page, len(records), total)
        parse(
            post(session, config.api_url, config, json=payload(page))
            .json()
            .get("responseEntries", [])
        )

    return records


def query_figshare(
    config: ApiFetcherConfig, session: requests.Session
) -> list[Record]:
    """
    Query handler for Figshare and Figshare-based repositories.

    Note: BIOcean5D's DTU-affiliated records are hosted on Figshare (DTU Data
    is a Figshare instance), so they are found through this handler rather
    than a DTU-specific one.

    Only `QueryTerm` is supported. Use ``field="search_for"`` for fulltext
    search across all metadata fields.

    Raises
    ------
    ValueError
        If ``config.query`` is a `QueryGroup`.
    """
    if isinstance(config.query, QueryGroup):
        raise ValueError(
            "Figshare only supports QueryTerm, not QueryGroup. "
            "Use field='search_for' for fulltext search."
        )

    page = 1
    records: list[Record] = []

    def payload(p: int) -> dict:
        return {
            config.query.field: config.query.value,
            "page_size": config.page_size,
            "page": p,
        }

    def parse(items: list[dict]) -> None:
        for item in items:
            records.append(
                Record(
                    doi=item.get("doi", "").lower() or None,
                    title=item.get("title", "(no title)"),
                    publisher=item.get("publisher", "Figshare"),
                    url=item.get("url_public_html", ""),
                    source_id=str(item.get("id", "")),
                )
            )

    items = post(session, config.api_url, config, json=payload(page)).json()
    parse(items)
    logger.info("Figshare: fetching records ...")
    # Figshare signals end-of-results with a page shorter than page_size
    while len(items) == config.page_size:
        page += 1
        logger.debug("Fetching page %d (%d records so far) ...", page, len(records))
        time.sleep(config.request_delay)
        items = post(session, config.api_url, config, json=payload(page)).json()
        parse(items)

    logger.info("Figshare: %d record(s) found", len(records))
    return records


# ---------------------------------------------------------------------------
# Query handler registry
# ---------------------------------------------------------------------------

type QueryHandler = Callable[[ApiFetcherConfig, requests.Session], list[Record]]

#: Maps a substring of ``api_url`` to the appropriate query handler.
#: To add support for a new repository, append a ``(pattern, handler)`` tuple.
QUERY_HANDLERS: list[tuple[str, QueryHandler]] = [
    ("zenodo.org/api", query_zenodo),
    ("api.datacite.org", query_datacite),
    ("seanoe.org/api", query_seanoe),
    ("api.figshare.com", query_figshare),
]


def find_query_handler(api_url: str) -> QueryHandler:
    """Return the query handler whose pattern matches ``api_url``."""
    for pattern, handler in QUERY_HANDLERS:
        if pattern in api_url:
            return handler
    raise ValueError(
        f"No query handler registered for API URL: {api_url!r}. "
        f"Registered patterns: {[p for p, _ in QUERY_HANDLERS]}"
    )


# ---------------------------------------------------------------------------
# Fetch handlers
# ---------------------------------------------------------------------------


def fetch_export_endpoint(
    record: Record,
    config: ApiFetcherConfig,
    session: requests.Session,
) -> dict:
    """Fetch JSON-LD from the URL produced by substituting ``record.source_id``
    into ``config.export_url_template``."""
    url = config.export_url_template.format(record_id=record.source_id)
    return get(session, url, config).json()


def fetch_html_jsonld(
    record: Record,
    config: ApiFetcherConfig,
    session: requests.Session,
) -> dict:
    """Fetch the record's landing page and extract the first
    ``<script type="application/ld+json">`` block."""
    if not record.url:
        raise ValueError(
            f"Record has no landing page URL: {record.doi or record.source_id}"
        )
    response = get(session, record.url, config)
    soup = BeautifulSoup(response.text, "html.parser")
    tag = soup.find("script", {"type": "application/ld+json"})
    if not tag or not tag.string:
        raise ValueError(f"No JSON-LD block found in page: {record.url}")
    return json.loads(tag.string)


type FetchHandler = Callable[[Record, ApiFetcherConfig, requests.Session], dict]

FETCH_HANDLERS: dict[str, FetchHandler] = {
    "export_endpoint": fetch_export_endpoint,
    "html_jsonld": fetch_html_jsonld,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def query_source(config: ApiFetcherConfig) -> list[Record]:
    """
    Query a repository API and return all matching records.

    Each call is independent — results are not merged or deduplicated.
    To query multiple sources or use multiple queries, call this function
    once per `ApiFetcherConfig` and handle deduplication in the calling code.

    Parameters
    ----------
    config :
        A `ApiFetcherConfig` specifying the API endpoint, query, and
        request behaviour.

    Returns
    -------
    list[Record]
        All matching records in API-returned order.

    Raises
    ------
    ValueError
        If the query type is unsupported for the target API, no query
        handler matches ``config.api_url``, or a response exceeds
        ``config.max_response_mb``.
    requests.HTTPError
        If the API returns an HTTP error response.
    requests.TooManyRedirects
        If the number of redirects exceeds ``config.max_redirects``.
    """
    session = make_session(config.user_agent, config.max_redirects)
    return find_query_handler(config.api_url)(config, session)


def fetch_jsonld(record: Record, config: ApiFetcherConfig) -> dict:
    """
    Retrieve JSON-LD metadata for a single record.

    The fetch strategy is determined by ``config.fetch_strategy``. The same
    `ApiFetcherConfig` used for discovery may be reused here, or a separate
    one may be passed if the fetch endpoint differs (e.g. when records were
    discovered via DataCite but JSON-LD is fetched from the originating
    repository).

    Parameters
    ----------
    record :
        A `Record` instance returned by `query_source`.
    config :
        A `ApiFetcherConfig` whose ``fetch_strategy`` field controls the
        fetch strategy.

    Returns
    -------
    dict
        The parsed JSON-LD document.

    Raises
    ------
    ValueError
        If no JSON-LD block is found, the record has no landing page URL,
        or a response exceeds ``config.max_response_mb``.
    requests.HTTPError
        If the HTTP request fails.
    requests.TooManyRedirects
        If the number of redirects exceeds ``config.max_redirects``.
    """
    session = make_session(config.user_agent, config.max_redirects)
    return FETCH_HANDLERS[config.fetch_strategy](record, config, session)
