"""
Source-agnostic metadata collection library.

Provides a unified interface for querying repository APIs and retrieving
JSON-LD metadata records. All behaviour is controlled via `CollectionConfig`
— no source-specific code paths are needed for supported repositories.

Supported repositories (built-in query handlers):

- Zenodo       (``zenodo.org/api``)
- DataCite     (``api.datacite.org``)
- SEANOE       (``seanoe.org/api``)
- Figshare and Figshare-based repositories such as DTU Data (``api.figshare.com``)

Supported extractors:

- ``"export_endpoint"`` — retrieves JSON-LD from a URL template containing ``{record_id}``
- ``"html_jsonld"``     — scrapes a ``<script type="application/ld+json">`` block
                          from the record's landing page

Extending to new sources
------------------------
Pagination is necessarily source-specific, as each API has its own request
format, response structure, and record schema. To add a new source, implement
a ``_query_*`` function following the existing pattern and register it in
``_QUERY_HANDLERS``. No changes to the public API are required unless a new
extractor type is needed.

Public API
----------
::

    query_source(config: CollectionConfig) -> list[Record]
    fetch_jsonld(record: Record, config: CollectionConfig) -> dict

Examples
--------
::

    from metadata_collector import (
        CollectionConfig, QueryTerm, QueryGroup, query_source, fetch_jsonld,
    )

    # Query the Zenodo BIOcean5D community
    records = query_source(CollectionConfig(
        api_url="https://zenodo.org/api/records",
        query=QueryTerm(field="communities", value="horizoneurope_biocean5d"),
        extractor="export_endpoint",
        export_url_template="https://zenodo.org/records/{record_id}/export/json-ld",
    ))

    # Query DataCite by award number or project name
    records = query_source(CollectionConfig(
        api_url="https://api.datacite.org/dois",
        query=QueryGroup(operator="OR", terms=[
            QueryTerm(field="fundingReferences.awardNumber", value="101059915"),
            QueryTerm(field="fundingReferences.awardTitle", value="BIOcean5D"),
        ]),
        extractor="html_jsonld",
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
import time
from collections.abc import Callable
from typing import Literal

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, field_validator, model_validator

# ---------------------------------------------------------------------------
# Query model
# ---------------------------------------------------------------------------

type Query = QueryTerm | QueryGroup


class QueryTerm(BaseModel):
    """
    A single field:value search term.

    Parameters
    ----------
    field :
        The API-specific field name to search (e.g. ``"communities"``).
    value :
        The value to match (e.g. ``"horizoneurope_biocean5d"``).
    """

    field: str
    value: str


class QueryGroup(BaseModel):
    """
    A logical group of `QueryTerm` or nested `QueryGroup` instances.

    Serialised to Elasticsearch query string syntax for Zenodo and DataCite
    (e.g. ``"(a:x OR b:y)"``). Sources that do not support compound queries
    (SEANOE, Figshare) raise `ValueError` at runtime if a `QueryGroup` is
    supplied.

    Parameters
    ----------
    operator :
        Logical operator joining all terms (``"AND"`` or ``"OR"``).
    terms :
        One or more `QueryTerm` or nested `QueryGroup` instances.
    """

    operator: Literal["AND", "OR"]
    terms: list["QueryTerm | QueryGroup"]


QueryGroup.model_rebuild()  # resolve forward reference


# ---------------------------------------------------------------------------
# Record
# ---------------------------------------------------------------------------

class Record(BaseModel):
    """
    A metadata record returned by `query_source`.

    Parameters
    ----------
    doi :
        Canonical DOI in lowercase, or ``None`` if unavailable.
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

    doi:       str | None
    title:     str
    publisher: str
    url:       str
    source_id: str


# ---------------------------------------------------------------------------
# CollectionConfig
# ---------------------------------------------------------------------------

class CollectionConfig(BaseModel):
    """
    Configuration for a single `query_source` or `fetch_jsonld` call.

    All behaviour is explicit — there are no hidden defaults derived from the
    source name or URL. The appropriate query handler is selected automatically
    from ``api_url`` via `_QUERY_HANDLERS`.

    Parameters
    ----------
    api_url :
        HTTPS URL of the repository search API endpoint.
    query :
        A `QueryTerm` or `QueryGroup` describing the search.
    extractor :
        Strategy for retrieving JSON-LD for each record:

        - ``"export_endpoint"`` — performs a GET request to
          ``export_url_template`` with ``{record_id}`` substituted.
        - ``"html_jsonld"``     — fetches the record's landing page and
          extracts the first ``<script type="application/ld+json">`` block.
    export_url_template :
        URL template used by the ``"export_endpoint"`` extractor.
        Must contain the literal placeholder ``{record_id}``.
        Ignored when ``extractor="html_jsonld"``.
    page_size :
        Number of records to request per API page. Defaults to ``25``,
        which is safe for unauthenticated Zenodo requests.
    request_delay :
        Seconds to sleep between paginated requests. Defaults to ``0.5``.
    user_agent :
        Value of the ``User-Agent`` HTTP header sent with every request.
    max_redirects :
        Maximum number of HTTP redirects to follow. Set to ``0`` to
        disallow redirects entirely. Defaults to ``0``.
    max_response_mb :
        Maximum acceptable response body size in megabytes. Requests
        exceeding this limit raise `ValueError`. Defaults to ``10.0``.
    """

    api_url:             str
    query:               Query
    extractor:           Literal["export_endpoint", "html_jsonld"]
    export_url_template: str   = "https://zenodo.org/records/{record_id}/export/json-ld"
    page_size:           int   = 25
    request_delay:       float = 0.5
    user_agent:          str   = "metadata-collector/1.0"
    max_redirects:       int   = 0
    max_response_mb:     float = 10.0

    @field_validator("api_url", "export_url_template", mode="before")
    @classmethod
    def _require_https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError(f"URL must use HTTPS: {v!r}")
        return v

    @model_validator(mode="after")
    def _export_template_has_placeholder(self) -> "CollectionConfig":
        if self.extractor == "export_endpoint" and "{record_id}" not in self.export_url_template:
            raise ValueError(
                "export_url_template must contain {record_id} "
                "when extractor='export_endpoint'."
            )
        return self


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _make_session(config: CollectionConfig) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": config.user_agent})
    session.max_redirects = config.max_redirects
    return session


def _checked(fn):
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
            session, url, config,
            allow_redirects=config.max_redirects > 0,
            timeout=30,
            **kwargs,
        )
        response.raise_for_status()
        _check_response_size(response, config)
        return response
    return wrapper


@_checked
def _get(session: requests.Session, url: str, config: CollectionConfig, **kwargs) -> requests.Response:
    """Perform a GET request."""
    return session.get(url, **kwargs)


@_checked
def _post(session: requests.Session, url: str, config: CollectionConfig, **kwargs) -> requests.Response:
    """Perform a POST request."""
    return session.post(url, **kwargs)


def _check_response_size(response: requests.Response, config: CollectionConfig) -> None:
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

def _to_es_query(query: Query) -> str:
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
            parts = [_to_es_query(t) for t in terms]
            return "(" + f" {op} ".join(parts) + ")"


# ---------------------------------------------------------------------------
# Query handlers
# ---------------------------------------------------------------------------

def _query_zenodo(config: CollectionConfig, session: requests.Session) -> list[Record]:
    """Query handler for the Zenodo REST API."""
    params: dict = {
        "q":    _to_es_query(config.query),
        "size": config.page_size,
        "page": 1,
        "sort": "newest",
    }
    data = _get(session, config.api_url, config, params=params).json()
    total = data["hits"]["total"]
    records: list[Record] = []

    def _parse(hits: list[dict]) -> None:
        for hit in hits:
            rec_id = str(hit["id"])
            records.append(Record(
                doi=hit.get("doi", "").lower() or None,
                title=hit["metadata"].get("title", "(no title)"),
                publisher="Zenodo",
                url=f"https://zenodo.org/records/{rec_id}",
                source_id=rec_id,
            ))

    _parse(data["hits"]["hits"])
    while len(records) < total:
        params["page"] += 1
        time.sleep(config.request_delay)
        _parse(_get(session, config.api_url, config, params=params).json()["hits"]["hits"])

    return records


def _query_datacite(config: CollectionConfig, session: requests.Session) -> list[Record]:
    """Query handler for the DataCite REST API."""
    params: dict = {
        "query":        _to_es_query(config.query),
        "page[size]":   config.page_size,
        "page[number]": 1,
    }
    data = _get(session, config.api_url, config, params=params).json()
    total = data["meta"]["total"]
    records: list[Record] = []

    def _parse(items: list[dict]) -> None:
        for item in items:
            attr = item["attributes"]
            doi = attr.get("doi", "").lower() or None
            title = attr["titles"][0]["title"] if attr.get("titles") else "(no title)"
            records.append(Record(
                doi=doi,
                title=title,
                publisher=attr.get("publisher", "Unknown"),
                url=attr.get("url", ""),
                source_id=doi or item["id"],
            ))

    _parse(data["data"])
    while len(records) < total:
        params["page[number]"] += 1
        time.sleep(config.request_delay)
        _parse(_get(session, config.api_url, config, params=params).json()["data"])

    return records


def _query_seanoe(config: CollectionConfig, session: requests.Session) -> list[Record]:
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

    def _payload(page: int) -> dict:
        return {
            "groupedSearch": True,
            "criteriaList": [
                {
                    "field": config.query.field,
                    "values": [{"code": config.query.value, "n": 0, "name": config.query.value}],
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

    data = _post(session, config.api_url, config, json=_payload(1)).json()
    total = data.get("entriesCount", 0)
    records: list[Record] = []

    def _parse(entries: list[dict]) -> None:
        for entry in entries:
            doc_id = str(entry.get("docId", ""))
            records.append(Record(
                doi=f"10.17882/{doc_id}" if doc_id else None,
                title=entry.get("title", entry.get("name", "(no title)")),
                publisher="SEANOE",
                url=entry.get("url", ""),
                source_id=doc_id,
            ))

    _parse(data.get("responseEntries", []))
    while len(records) < total:
        time.sleep(config.request_delay)
        page = len(records) // config.page_size + 1
        _parse(_post(session, config.api_url, config, json=_payload(page)).json().get("responseEntries", []))

    return records


def _query_figshare(config: CollectionConfig, session: requests.Session) -> list[Record]:
    """
    Query handler for Figshare and Figshare-based repositories
    (e.g. DTU Data at ``data.dtu.dk``).

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

    def _payload(p: int) -> dict:
        return {config.query.field: config.query.value, "page_size": config.page_size, "page": p}

    def _parse(items: list[dict]) -> None:
        for item in items:
            records.append(Record(
                doi=item.get("doi", "").lower() or None,
                title=item.get("title", "(no title)"),
                publisher=item.get("publisher", "Figshare"),
                url=item.get("url_public_html", ""),
                source_id=str(item.get("id", "")),
            ))

    items = _post(session, config.api_url, config, json=_payload(page)).json()
    _parse(items)
    # Figshare signals end-of-results with a page shorter than page_size
    while len(items) == config.page_size:
        page += 1
        time.sleep(config.request_delay)
        items = _post(session, config.api_url, config, json=_payload(page)).json()
        _parse(items)

    return records


# ---------------------------------------------------------------------------
# Query handler registry
# ---------------------------------------------------------------------------

type QueryHandler = Callable[[CollectionConfig, requests.Session], list[Record]]

#: Maps a substring of ``api_url`` to the appropriate query handler.
#: To add support for a new repository, append a ``(pattern, handler)`` tuple.
_QUERY_HANDLERS: list[tuple[str, QueryHandler]] = [
    ("zenodo.org/api",   _query_zenodo),
    ("api.datacite.org", _query_datacite),
    ("seanoe.org/api",   _query_seanoe),
    ("api.figshare.com", _query_figshare),
]


def _find_query_handler(api_url: str) -> QueryHandler:
    """Return the query handler whose pattern matches ``api_url``."""
    for pattern, handler in _QUERY_HANDLERS:
        if pattern in api_url:
            return handler
    raise ValueError(
        f"No query handler registered for API URL: {api_url!r}. "
        f"Registered patterns: {[p for p, _ in _QUERY_HANDLERS]}"
    )


# ---------------------------------------------------------------------------
# Fetch handlers
# ---------------------------------------------------------------------------

def _fetch_export_endpoint(
    record: Record,
    config: CollectionConfig,
    session: requests.Session,
) -> dict:
    """Fetch JSON-LD from the URL produced by substituting ``record.source_id``
    into ``config.export_url_template``."""
    url = config.export_url_template.format(record_id=record.source_id)
    return _get(session, url, config).json()


def _fetch_html_jsonld(
    record: Record,
    config: CollectionConfig,
    session: requests.Session,
) -> dict:
    """Fetch the record's landing page and extract the first
    ``<script type="application/ld+json">`` block."""
    if not record.url:
        raise ValueError(
            f"Record has no landing page URL: {record.doi or record.source_id}"
        )
    response = _get(session, record.url, config)
    soup = BeautifulSoup(response.text, "html.parser")
    tag = soup.find("script", {"type": "application/ld+json"})
    if not tag or not tag.string:
        raise ValueError(f"No JSON-LD block found in page: {record.url}")
    return json.loads(tag.string)


type FetchHandler = Callable[[Record, CollectionConfig, requests.Session], dict]

_FETCH_HANDLERS: dict[str, FetchHandler] = {
    "export_endpoint": _fetch_export_endpoint,
    "html_jsonld":     _fetch_html_jsonld,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def query_source(config: CollectionConfig) -> list[Record]:
    """
    Query a repository API and return all matching records.

    Each call is independent — results are not merged or deduplicated.
    To query multiple sources or use multiple queries, call this function
    once per `CollectionConfig` and handle deduplication in the calling code.

    Parameters
    ----------
    config :
        A `CollectionConfig` specifying the API endpoint, query, and
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
    session = _make_session(config)
    return _find_query_handler(config.api_url)(config, session)


def fetch_jsonld(record: Record, config: CollectionConfig) -> dict:
    """
    Retrieve JSON-LD metadata for a single record.

    The fetch strategy is determined by ``config.extractor``. The same
    `CollectionConfig` used for discovery may be reused here, or a separate
    one may be passed if the fetch endpoint differs (e.g. when records were
    discovered via DataCite but JSON-LD is fetched from the originating
    repository).

    Parameters
    ----------
    record :
        A `Record` instance returned by `query_source`.
    config :
        A `CollectionConfig` whose ``extractor`` field controls the
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
    session = _make_session(config)
    return _FETCH_HANDLERS[config.extractor](record, config, session)