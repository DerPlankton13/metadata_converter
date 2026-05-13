from typing import Literal

from pydantic import BaseModel


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
type Query = QueryTerm | QueryGroup
