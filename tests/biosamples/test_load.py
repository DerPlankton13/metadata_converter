import copy
import json
import re

import pytest

from metadata_converter.biosamples.fetch import fuse_metadata
from metadata_converter.biosamples.run import fix_obi, load_biosamples
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


def test_fix_obi_expands_prefix_and_drops_context_entry():
    fused = {
        "@context": [
            "http://schema.org",
            {
                "OBI": "http://purl.obolibrary.org/obo/OBI_",
                "biosample": "http://identifiers.org/biosample/",
            },
        ],
        "identifier": "biosample:SAMEA1",
        "mainEntity": {"@type": ["Sample", "OBI:0000747"]},
    }

    result = fix_obi(fused)

    assert result == {
        "@context": [
            "http://schema.org",
            {"biosample": "http://identifiers.org/biosample/"},
        ],
        "identifier": "biosample:SAMEA1",
        "mainEntity": {
            "@type": ["Sample", "http://purl.obolibrary.org/obo/OBI_0000747"]
        },
    }


def test_fix_obi_leaves_mid_string_occurrence_untouched():
    fused = {
        "@context": [
            "http://schema.org",
            {"OBI": "http://purl.obolibrary.org/obo/OBI_"},
        ],
        "note": "see reference OBI:0000747",
    }

    result = fix_obi(fused)

    assert result == {
        "@context": ["http://schema.org", {}],
        "note": "see reference OBI:0000747",
    }


def test_fix_obi_no_obi_key_returns_unchanged():
    fused = {
        "@context": [
            "http://schema.org",
            {"biosample": "http://identifiers.org/biosample/"},
        ],
        "stray": "OBI:0000747",
    }
    original = copy.deepcopy(fused)

    result = fix_obi(fused)

    assert result == original


def test_fix_obi_missing_context_returns_unchanged():
    fused = {"@type": "DataRecord", "identifier": "biosample:SAMEA1"}
    original = copy.deepcopy(fused)

    result = fix_obi(fused)

    assert result == original
