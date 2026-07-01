"""Tests for ``write_provenance_file``: each metadata file gets a per-record
provenance file recording what it is ``about``, what it ``isBasedOn`` (both as
``@id`` node refs), and which ``stage`` produced it."""
import json
import re

from metadata_converter.utils.provenance_writer import write_provenance_file


def test_provenance_writes_document(tmp_path):
    write_provenance_file(
        "Dataset_abc.jsonld",
        tmp_path,
        "https://zenodo.org/records/abc/export/json-ld",
        "load",
    )

    doc = json.loads((tmp_path / "Provenance_load_Dataset_abc.jsonld").read_text())
    assert doc["@context"] == {"@vocab": "https://schema.org/"}
    assert doc["@type"] == "DigitalDocument"
    assert doc["@id"] == "Provenance_load_Dataset_abc.jsonld"
    assert doc["about"] == {"@type": "Thing", "@id": "Dataset_abc.jsonld"}
    assert doc["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "https://zenodo.org/records/abc/export/json-ld",
    }
    assert doc["description"] == "stage: load"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", doc["dateCreated"])


def test_provenance_records_multiple_sources(tmp_path):
    write_provenance_file(
        "SAMEA1.jsonld",
        tmp_path,
        [
            "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.ldjson",
            "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.json",
        ],
        "load",
    )

    doc = json.loads((tmp_path / "Provenance_load_SAMEA1.jsonld").read_text())
    assert doc["isBasedOn"] == [
        {"@type": "CreativeWork", "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.ldjson"},
        {"@type": "CreativeWork", "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.json"},
    ]
