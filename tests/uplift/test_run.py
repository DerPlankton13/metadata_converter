"""Tests for ``run_uplift``'s provenance side-effect: each uplifted entity gets a
provenance file recording the loaded entity (by @id) it was based on. Since uplift
refines an entity in place, isBasedOn's @id equals about's @id — the stage
distinguishes them."""
import json

from metadata_converter.uplift import run_uplift


def test_uplift_writes_provenance(config_factory, tmp_path):
    cfg = config_factory(provenance_dir=tmp_path / "provenance")

    run_uplift(cfg)

    provenance_files = list((tmp_path / "provenance").glob("*.jsonld"))
    assert len(provenance_files) == 6

    alice = json.loads(
        (tmp_path / "provenance" / "Provenance_uplift_Person_alice.jsonld").read_text()
    )
    assert alice["about"] == {"@type": "Thing", "@id": "Person_alice.jsonld"}
    assert alice["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "Person_alice.jsonld",
    }
    assert alice["description"] == "stage: uplift"

    # A second, distinct entity must point at its own @id, not alice's —
    # guards against a constant isBasedOn/about written for every entity.
    bob = json.loads(
        (tmp_path / "provenance" / "Provenance_uplift_Person_bob.jsonld").read_text()
    )
    assert bob["about"] == {"@type": "Thing", "@id": "Person_bob.jsonld"}
    assert bob["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "Person_bob.jsonld",
    }
    assert bob["description"] == "stage: uplift"


def test_uplift_without_provenance_dir_writes_nothing(config_factory, tmp_path):
    cfg = config_factory(provenance_dir=None)

    run_uplift(cfg)

    assert not (tmp_path / "provenance").exists()
