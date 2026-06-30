"""Tests for ``write_provenance_file``: each metadata file gets a per-record sidecar
recording what it is ``about``, what it ``isBasedOn`` (both as ``@id`` node refs),
and which ``stage`` produced it."""
import json
import re

from metadata_converter.utils.provenance_writer import write_provenance_file


def test_provenance_writes_document(tmp_path):
    write_provenance_file(
        "Person_alice.jsonld",
        tmp_path,
        "https://www.ebi.ac.uk/biosamples/samples/SAMEA1",
        "load",
    )

    doc = json.loads((tmp_path / "Provenance_Person_alice.jsonld").read_text())
    assert doc["@context"] == {"@vocab": "https://schema.org/"}
    assert doc["@type"] == "DigitalDocument"
    assert doc["@id"] == "Provenance_Person_alice.jsonld"
    assert doc["about"] == {"@type": "Thing", "@id": "Person_alice.jsonld"}
    assert doc["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1",
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

    doc = json.loads((tmp_path / "Provenance_SAMEA1.jsonld").read_text())
    assert doc["isBasedOn"] == [
        {"@type": "CreativeWork", "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.ldjson"},
        {"@type": "CreativeWork", "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.json"},
    ]
