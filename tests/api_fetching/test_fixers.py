"""Tests for source-specific JSON-LD repair fixers applied before schema validation."""
from metadata_converter.api_fetching.fixers import FIXERS, fix_zenodo_funding_url


def test_fix_zenodo_funding_url_unwraps_url_object_in_list():
    jsonld = {
        "@type": "Dataset",
        "funding": [
            {
                "name": "Some Grant",
                "url": {
                    "identifier": "https://cordis.europa.eu/projects/101059915",
                    "scheme": "url",
                },
            }
        ],
    }

    fixed = fix_zenodo_funding_url(jsonld)

    assert fixed["funding"][0]["url"] == "https://cordis.europa.eu/projects/101059915"


def test_fix_zenodo_funding_url_unwraps_url_object_in_single_dict():
    jsonld = {
        "@type": "Dataset",
        "funding": {
            "name": "Some Grant",
            "url": {
                "identifier": "https://cordis.europa.eu/projects/862923",
                "scheme": "url",
            },
        },
    }

    fixed = fix_zenodo_funding_url(jsonld)

    assert fixed["funding"]["url"] == "https://cordis.europa.eu/projects/862923"


def test_fix_zenodo_funding_url_leaves_well_formed_url_untouched():
    jsonld = {
        "@type": "Dataset",
        "funding": [{"name": "Some Grant", "url": "https://cordis.europa.eu/projects/1"}],
    }

    fixed = fix_zenodo_funding_url(jsonld)

    assert fixed["funding"][0]["url"] == "https://cordis.europa.eu/projects/1"


def test_fix_zenodo_funding_url_no_funding_field_is_noop():
    jsonld = {"@type": "Dataset", "name": "no funding here"}

    fixed = fix_zenodo_funding_url(jsonld)

    assert fixed == {"@type": "Dataset", "name": "no funding here"}


def test_fixers_registry_contains_zenodo_fixer():
    assert FIXERS["fix_zenodo_funding_url"] is fix_zenodo_funding_url