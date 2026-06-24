"""Tests for ``EntityStore`` in ``uplift/entity_store.py``: load → write round-trip,
and loading from one or several input directories."""
import json
from pathlib import Path

import pytest

from metadata_converter.flat_data.uplift.entity_store import EntityStore


def write_jsonld(path: Path, type_name: str, entity_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "@context": {"@vocab": "https://schema.org"},
        "@type": type_name,
        "@id": entity_id,
    }))


def test_uplifted_corpus_matches_expected_files(uplifted):
    assert set(uplifted) == {
        "Person_alice.jsonld",
        "Person_bob.jsonld",
        "DataCatalog_main.jsonld",
        "Action_analysis1.jsonld",
        "Dataset_file1.jsonld",
        "Product_SAMEA0001.jsonld",
    }


# ---------------------------------------------------------------------------
# EntityStore.load — single dir and multiple dirs
# ---------------------------------------------------------------------------


def test_load_single_dir_accepts_path(tmp_path):
    input_dir = tmp_path / "in"
    write_jsonld(input_dir / "Person_alice.jsonld", "Person", "Person_alice.jsonld")
    write_jsonld(input_dir / "Dataset_f1.jsonld", "Dataset", "Dataset_f1.jsonld")

    store = EntityStore.load(input_dir)

    [person] = store.of_type("Person")
    [dataset] = store.of_type("Dataset")
    assert person.id == "Person_alice.jsonld"
    assert dataset.id == "Dataset_f1.jsonld"


def test_load_multiple_dirs_merges_same_type(tmp_path):
    input_dir_a = tmp_path / "a"
    input_dir_b = tmp_path / "b"
    write_jsonld(input_dir_a / "Person_alice.jsonld", "Person", "Person_alice.jsonld")
    write_jsonld(input_dir_b / "Person_bob.jsonld", "Person", "Person_bob.jsonld")

    store = EntityStore.load([input_dir_a, input_dir_b])

    people = store.of_type("Person")
    assert {p.id for p in people} == {"Person_alice.jsonld", "Person_bob.jsonld"}


def test_load_duplicate_id_across_dirs_raises(tmp_path):
    input_dir_a = tmp_path / "a"
    input_dir_b = tmp_path / "b"
    write_jsonld(input_dir_a / "Person_alice.jsonld", "Person", "Person_alice.jsonld")
    write_jsonld(input_dir_b / "Person_alice.jsonld", "Person", "Person_alice.jsonld")

    with pytest.raises(ValueError, match="Person_alice.jsonld"):
        EntityStore.load([input_dir_a, input_dir_b])
