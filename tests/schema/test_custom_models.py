"""Tests for the DOI/ISSN/ISBN custom PropertyValue subclasses.

Each class derives ``url`` from ``value`` in a ``model_validator(mode="before")`` —
mirroring ``Orcid.clean_id`` — so that no post-construction assignment is ever made
(assigning ``self.url`` on an already-built, ``validate_assignment=True`` model would
re-trigger the same validator and recurse infinitely).
"""
import pytest
from pydantic import ValidationError

from metadata_converter.schema_org_models.custom_models import DOI, ISBN, ISSN


@pytest.mark.parametrize(
    "raw_value, expected_value, expected_url",
    [
        (
            "10.5281/zenodo.15739159",
            "10.5281/zenodo.15739159",
            "https://doi.org/10.5281/zenodo.15739159",
        ),
        (
            "https://doi.org/10.5281/zenodo.15739159",
            "10.5281/zenodo.15739159",
            "https://doi.org/10.5281/zenodo.15739159",
        ),
        (
            "dx.doi.org/10.17504/protocols.io.bp2l6bq3zgqe/v4",
            "10.17504/protocols.io.bp2l6bq3zgqe/v4",
            "https://doi.org/10.17504/protocols.io.bp2l6bq3zgqe/v4",
        ),
        (
            "doi:10.1234/xyz",
            "10.1234/xyz",
            "https://doi.org/10.1234/xyz",
        ),
    ],
)
def test_doi_extracts_bare_value_from_various_forms(
    raw_value, expected_value, expected_url
):
    doi = DOI(value=raw_value)

    assert doi.value == expected_value
    assert doi.url == expected_url


def test_doi_non_doi_text_raises():
    with pytest.raises(ValidationError, match="Invalid DOI"):
        DOI(
            value=(
                "Data and code available via zenodo "
                "https://zenodo.org/records/15168238 (Schickele & Hofmann Elizondo, 2025)."
            )
        )


def test_issn_valid_value_sets_url():
    issn = ISSN(value="2049-3630")

    assert issn.value == "2049-3630"
    assert issn.url == "https://portal.issn.org/resource/ISSN/2049-3630"


def test_issn_invalid_value_raises():
    with pytest.raises(ValidationError, match="Invalid ISSN"):
        ISSN(value="not-an-issn")


def test_isbn_valid_value_sets_url():
    isbn = ISBN(value="978-3-16-148410-0")

    assert isbn.value == "978-3-16-148410-0"
    assert isbn.url == "https://isbnsearch.org/isbn/978-3-16-148410-0"


def test_isbn_invalid_value_raises():
    with pytest.raises(ValidationError, match="Invalid ISBN"):
        ISBN(value="not-an-isbn")
