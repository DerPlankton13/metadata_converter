import json
import re
import shutil

import pytest

from metadata_converter.biosamples.fetch import fuse_metadata
from metadata_converter.biosamples.run import load_biosamples
from tests.biosamples.conftest import (
    DATA_DIR,
    assert_no_diff,
    biosamples_config,
    load_json,
    strip_none,
    write_fetched_sample,
)

SAMPLE_IDS = ["SAMEA112489011", "SAMEA111477556", "SAMEA118673980"]


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_load_sample(sample_id):
    expected = load_json(DATA_DIR / f"{sample_id}_with_units.jsonld")
    structured = load_json(DATA_DIR / f"{sample_id}.ldjson")
    unstructured = load_json(DATA_DIR / f"{sample_id}.json")

    result = fuse_metadata(structured, unstructured)
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected), strip_none(result))


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_load_biosamples_writes_expected_creativework(tmp_path, sample_id):
    expected = load_json(DATA_DIR / f"CreativeWork_{sample_id}.jsonld")
    fetched_dir = tmp_path / "fetched"
    fetched_dir.mkdir()
    shutil.copy(DATA_DIR / f"{sample_id}.ldjson", fetched_dir)
    shutil.copy(DATA_DIR / f"{sample_id}.json", fetched_dir)
    config = biosamples_config(fetched_dir, tmp_path / "loaded", None)

    load_biosamples(config)

    result = load_json(tmp_path / "loaded" / f"CreativeWork_{sample_id}.jsonld")
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected), strip_none(result))


def test_load_biosamples_writes_provenance(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fetched_dir = tmp_path / "fetched"
    fetched_dir.mkdir()
    write_fetched_sample(fetched_dir, "SAMEA1")
    config = biosamples_config(
        fetched_dir, tmp_path / "loaded_base", tmp_path / "provenance"
    )

    load_biosamples(config)

    provenance = json.loads(
        (
            tmp_path / "provenance" / "Provenance_load_CreativeWork_SAMEA1.jsonld"
        ).read_text()
    )
    assert provenance == {
        "@context": {"@vocab": "https://schema.org/"},
        "@type": "DigitalDocument",
        "@id": "Provenance_load_CreativeWork_SAMEA1.jsonld",
        "about": {
            "@type": "Thing",
            "@id": "CreativeWork_SAMEA1.jsonld",
        },
        "isBasedOn": [
            {
                "@type": "CreativeWork",
                "@id": "http://identifiers.org/biosample/SAMEA1",
            },
            {"@type": "CreativeWork", "@id": "fetched/SAMEA1.json"},
        ],
        "description": "stage: load",
        "dateCreated": provenance["dateCreated"],
    }
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", provenance["dateCreated"]
    )


def test_load_biosamples_without_provenance_dir_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    fetched_dir = tmp_path / "fetched"
    fetched_dir.mkdir()
    write_fetched_sample(fetched_dir, "SAMEA1")
    config = biosamples_config(fetched_dir, tmp_path / "loaded_base", None)

    load_biosamples(config)

    assert not (tmp_path / "provenance").exists()
