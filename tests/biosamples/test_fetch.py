import json
import re

from metadata_converter.biosamples.fetch import get_metadata, sample_source_urls

# ---------------------------------------------------------------------------
# Provenance: source URLs and per-record provenance files
# ---------------------------------------------------------------------------


def test_sample_source_urls():
    assert sample_source_urls("SAMEA1") == [
        "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.ldjson",
        "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.json",
    ]


def fake_fetch_metadata(url: str, session) -> dict:
    """Stand-in for fetch.fetch_metadata: returns canned structured/unstructured
    metadata for sample SAMEA1 without touching the network."""
    if url.endswith(".ldjson"):
        return {
            "@id": "biosample:SAMEA1",
            "@context": [
                "http://schema.org",
                {"biosample": "http://identifiers.org/biosample/"},
            ],
        }
    return {"characteristics": {}}


def test_get_metadata_writes_fetch_provenance(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "metadata_converter.biosamples.fetch.fetch_metadata", fake_fetch_metadata
    )
    (tmp_path / "fetched").mkdir()

    get_metadata(
        "SAMEA1",
        session=None,
        fetched_path=tmp_path / "fetched",
        provenance_dir=tmp_path / "provenance",
    )

    ldjson_provenance = json.loads(
        (tmp_path / "provenance" / "Provenance_fetch_SAMEA1.jsonld").read_text()
    )
    assert ldjson_provenance == {
        "@context": {"@vocab": "https://schema.org/"},
        "@type": "DigitalDocument",
        "@id": "Provenance_fetch_SAMEA1",
        "about": {"@type": "Thing", "@id": "http://identifiers.org/biosample/SAMEA1"},
        "isBasedOn": {
            "@type": "CreativeWork",
            "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.ldjson",
        },
        "description": "stage: fetch",
        "dateCreated": ldjson_provenance["dateCreated"],
    }
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", ldjson_provenance["dateCreated"]
    )

    json_provenance = json.loads(
        (tmp_path / "provenance" / "Provenance_fetch_SAMEA1.json.jsonld").read_text()
    )
    assert json_provenance == {
        "@context": {"@vocab": "https://schema.org/"},
        "@type": "DigitalDocument",
        "@id": "Provenance_fetch_SAMEA1.json",
        "about": {"@type": "Thing", "@id": "fetched/SAMEA1.json"},
        "isBasedOn": {
            "@type": "CreativeWork",
            "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.json",
        },
        "description": "stage: fetch",
        "dateCreated": json_provenance["dateCreated"],
    }
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", json_provenance["dateCreated"]
    )


def test_get_metadata_without_provenance_dir_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "metadata_converter.biosamples.fetch.fetch_metadata", fake_fetch_metadata
    )
    (tmp_path / "fetched").mkdir()

    get_metadata(
        "SAMEA1",
        session=None,
        fetched_path=tmp_path / "fetched",
        provenance_dir=None,
    )

    assert not (tmp_path / "provenance").exists()