"""Tests for the api-fetching run step. Provenance is written during fetch, where
the exact source URL each record was pulled from is known."""
import json
import re

from metadata_converter.api_fetching.fetch import Record
from metadata_converter.api_fetching.query_models import QueryTerm
from metadata_converter.api_fetching.run import fetch_api_data
from metadata_converter.config import (
    ApiFetcherConfig,
    ApiFetchingConfig,
)


def api_config(fetched, loaded_base, provenance_dir, fetch_strategy="export_endpoint"):
    """A minimal ApiFetchingConfig for the fetch step."""
    return ApiFetchingConfig(
        fetcher=ApiFetcherConfig(
            api_url="https://example.org/api",
            query=QueryTerm(field="communities", value="x"),
            fetch_strategy=fetch_strategy,
            export_url_template="https://zenodo.org/records/{record_id}/export/json-ld",
        ),
        fetched_dir=fetched,
        output_dir=loaded_base,
        provenance_dir=provenance_dir,
    )


def patch_query_and_fetch(monkeypatch, record):
    """Stubs the network so the source query yields this one record, fetched as a Dataset."""
    monkeypatch.setattr(
        "metadata_converter.api_fetching.run.query_source", lambda extractor: [record]
    )
    monkeypatch.setattr(
        "metadata_converter.api_fetching.run.fetch_jsonld",
        lambda rec, extractor: {"@type": "Dataset", "@id": "Dataset_rec1.jsonld"},
    )


def test_api_fetch_export_endpoint_records_export_url(tmp_path, monkeypatch):
    config = api_config(
        tmp_path / "fetched", tmp_path / "loaded_base", tmp_path / "provenance"
    )
    record = Record(
        doi="10.x/1", title="T", publisher="P",
        url="https://zenodo.org/records/rec1", source_id="rec1",
    )
    patch_query_and_fetch(monkeypatch, record)

    fetch_api_data(config)

    doc = json.loads(
        (tmp_path / "provenance" / "Provenance_load_Dataset_rec1.jsonld").read_text()
    )
    assert doc == {
        "@context": {"@vocab": "https://schema.org/"},
        "@type": "DigitalDocument",
        "@id": "Provenance_load_Dataset_rec1.jsonld",
        "about": {"@type": "Thing", "@id": "Dataset_rec1.jsonld"},
        "isBasedOn": {
            "@type": "CreativeWork",
            "@id": "https://zenodo.org/records/rec1/export/json-ld",
        },
        "description": "stage: load",
        "dateCreated": doc["dateCreated"],
    }
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", doc["dateCreated"])


def test_api_fetch_html_jsonld_records_landing_url(tmp_path, monkeypatch):
    config = api_config(
        tmp_path / "fetched", tmp_path / "loaded_base", tmp_path / "provenance",
        fetch_strategy="html_jsonld",
    )
    record = Record(
        doi="10.x/1", title="T", publisher="P",
        url="https://seanoe.org/data/rec1", source_id="rec1",
    )
    patch_query_and_fetch(monkeypatch, record)

    fetch_api_data(config)

    doc = json.loads(
        (tmp_path / "provenance" / "Provenance_load_Dataset_rec1.jsonld").read_text()
    )
    assert doc["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "https://seanoe.org/data/rec1",
    }


def test_api_fetch_without_provenance_dir_writes_nothing(tmp_path, monkeypatch):
    config = api_config(tmp_path / "fetched", tmp_path / "loaded_base", None)
    record = Record(
        doi="10.x/1", title="T", publisher="P",
        url="https://zenodo.org/records/rec1", source_id="rec1",
    )
    patch_query_and_fetch(monkeypatch, record)

    fetch_api_data(config)

    assert not (tmp_path / "provenance").exists()
