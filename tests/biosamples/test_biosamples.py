import json
import re
from pathlib import Path

import pytest
from deepdiff import DeepDiff

from metadata_converter.biosamples.fetch import fuse_metadata, sample_source_urls
from metadata_converter.biosamples.run import fetch_biosamples
from metadata_converter.config import (
    BiosamplesConfig,
    BiosamplesInput,
    FetchedOutputConfig,
)
from metadata_converter.biosamples.uplifting import (
    ActionBuilder,
    SampleRecord,
    SampleUplifter,
    build_defined_term,
    build_property,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SAMPLE_IDS = ["SAMEA112489011", "SAMEA111477556", "SAMEA118673980"]
DATA_DIR = Path(__file__).parent / "data"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def assert_no_diff(expected: dict, result: dict):
    diff = DeepDiff(expected, result, ignore_order=False)
    if diff:
        print("result:")
        print(json.dumps(result, indent=2))
        pytest.fail(diff.pretty())


def strip_none(d: dict) -> dict:
    def process(v):
        if isinstance(v, dict):
            return strip_none(v)
        if isinstance(v, list):
            return [process(i) for i in v]
        return v

    return {k: process(v) for k, v in d.items() if v is not None}


def make_record(*props: dict) -> dict:
    return {
        "@id": "biosample:SAMEA000000",
        "mainEntity": {"additionalProperty": list(props)},
    }


def make_property(name: str, value: str, value_reference=None) -> dict:
    prop = {"@type": "PropertyValue", "name": name, "value": value}
    if value_reference is not None:
        prop["valueReference"] = value_reference
    return prop


def make_coord_property(name: str, value: str, unit: str) -> dict:
    return {"@type": "PropertyValue", "name": name, "value": value, "unitText": unit}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_fuse_metadata(sample_id):
    expected = load_json(DATA_DIR / f"{sample_id}_with_units.jsonld")
    structured = load_json(DATA_DIR / f"{sample_id}_original.jsonld")
    unstructured = load_json(DATA_DIR / f"{sample_id}_original.json")

    result = fuse_metadata(structured, unstructured)
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected), strip_none(result))


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_extract_product(sample_id):
    expected = load_json(DATA_DIR / f"Product_{sample_id}.jsonld")
    data = load_json(DATA_DIR / f"{sample_id}_with_units.jsonld")

    product, _ = SampleUplifter(data).build_dicts()
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected), strip_none(product))


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_extract_action(sample_id):
    expected = load_json(DATA_DIR / f"Action_{sample_id}.jsonld")
    data = load_json(DATA_DIR / f"{sample_id}_with_units.jsonld")

    _, action = SampleUplifter(data).build_dicts()
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected), strip_none(action))


EXPECTED_ENVO = {
    "@type": "DefinedTerm",
    "name": "organism name",
    "termCode": "ENVO:12345",
    "url": "https://purl.obolibrary.org/obo/ENVO_12345",
    "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
}

EXPECTED_NCBI = {
    "@type": "DefinedTerm",
    "name": "marine metagenome",
    "termCode": "12345",
    "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=12345",
    "inDefinedTermSet": "https://www.ncbi.nlm.nih.gov/Taxonomy",
}


@pytest.mark.parametrize(
    ("input", "expected"),
    [
        pytest.param(
            "organism name [ENVO:12345]", EXPECTED_ENVO, id="envo_square_brackets"
        ),
        pytest.param(
            "organism name (ENVO:12345)", EXPECTED_ENVO, id="envo_round_brackets"
        ),
        pytest.param(
            "I am a (random) description of weired properties [1234]",
            None,
            id="multiple_brackets_returns_none",
        ),
        pytest.param(
            "marine metagenome [NCBI:txid12345]",
            EXPECTED_NCBI,
            id="ncbi_square_brackets",
        ),
        pytest.param(
            "marine metagenome (NCBI:txid12345)",
            EXPECTED_NCBI,
            id="ncbi_round_brackets",
        ),
        pytest.param(
            "nice property [prop:1234]", None, id="unknown_terminology_returns_none"
        ),
    ],
)
def test_build_defined_term(input, expected):
    defined_term = build_defined_term(input)
    assert_no_diff(expected, defined_term)


def test_build_property_multi_value():
    record = make_record(
        make_property(
            name="broad-scale environmental context",
            value="terrestrial biome [ENVO:00000446]|forest biome [ENVO:01000174]|coastal scrubland [ENVO:01000237]",
            value_reference=[
                {
                    "@id": "http://purl.obolibrary.org/obo/ENVO_00000446",
                    "@type": "DefinedTerm",
                },
                {
                    "@id": "http://purl.obolibrary.org/obo/ENVO_01000174",
                    "@type": "DefinedTerm",
                },
                {
                    "@id": "http://purl.obolibrary.org/obo/ENVO_01000237",
                    "@type": "DefinedTerm",
                },
            ],
        )
    )
    expected = {
        "@type": "PropertyValue",
        "name": "broad-scale environmental context",
        "value": [
            "terrestrial biome [ENVO:00000446]",
            "forest biome [ENVO:01000174]",
            "coastal scrubland [ENVO:01000237]",
        ],
        "valueReference": [
            {
                "@type": "DefinedTerm",
                "name": "terrestrial biome",
                "termCode": "ENVO:00000446",
                "url": "https://purl.obolibrary.org/obo/ENVO_00000446",
                "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
            },
            {
                "@type": "DefinedTerm",
                "name": "forest biome",
                "termCode": "ENVO:01000174",
                "url": "https://purl.obolibrary.org/obo/ENVO_01000174",
                "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
            },
            {
                "@type": "DefinedTerm",
                "name": "coastal scrubland",
                "termCode": "ENVO:01000237",
                "url": "https://purl.obolibrary.org/obo/ENVO_01000237",
                "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
            },
        ],
    }
    result = build_property(record, "broad-scale environmental context")
    assert_no_diff(expected, result)


def test_build_property_fallback_fixes_https():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={
                "@type": "DefinedTerm",
                "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
            },
        )
    )
    result = build_property(record, "sample collection device")
    assert result["valueReference"] == {
        "@type": "DefinedTerm",
        "@id": "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
    }


def test_build_property_fallback_removes_empty_value_reference():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={"@type": "DefinedTerm"},
        )
    )
    result = build_property(record, "sample collection device")
    assert "valueReference" not in result


def test_build_property_fallback_removes_value_reference_without_information():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={"@type": "DefinedTerm", "@id": ""},
        )
    )
    result = build_property(record, "sample collection device")
    assert "valueReference" not in result


def test_build_property_raises_for_multi_element_single_value():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference=[
                {"@type": "DefinedTerm", "@id": "http://example.com/1"},
                {"@type": "DefinedTerm", "@id": "http://example.com/2"},
            ],
        )
    )
    with pytest.raises(ValueError):
        build_property(record, "sample collection device")


def test_build_location_both_region_and_country():
    record = make_record(
        make_property("geographic location (region and locality)", "Ligurian Sea"),
        make_property("geographic location (country and/or sea)", "Mediterranean Sea"),
        make_property("sampling design label", "SDL-001"),
    )
    expected = {
        "@type": "Place",
        "name": "Ligurian Sea, Mediterranean Sea",
        "geo": None,
        "additionalProperty": [
            {
                "@type": "PropertyValue",
                "propertyID": "https://w3id.org/mixs/0000010",
                "name": "geographic location (country and/or sea,region)",
                "value": "Mediterranean Sea: , Ligurian Sea",
            },
            {
                "@type": "PropertyValue",
                "name": "sampling design label",
                "description": "Sampling Design Label (SDL) is a unique identifier used to track all samples and data originating from the same sampling location. (https://biocean5d.embl.de/faq.cgi)",
                "propertyID": "sampling design label",
                "value": "SDL-001",
            },
        ],
    }
    result = ActionBuilder(SampleRecord(record)).build_location()
    assert_no_diff(expected, result)


def test_build_location_country_only():
    record = make_record(
        make_property("geographic location (country and/or sea)", "Mediterranean Sea"),
    )
    expected = {
        "@type": "Place",
        "name": "Mediterranean Sea",
        "geo": None,
        "additionalProperty": None,
    }
    result = ActionBuilder(SampleRecord(record)).build_location()
    assert_no_diff(expected, result)


def test_build_location_with_coordinates():
    record = make_record(
        make_coord_property("geographic location (latitude)", "43.5", "DD"),
        make_coord_property("geographic location (longitude)", "7.8", "DD"),
        make_coord_property("elevation", "10", "m"),
    )
    result = ActionBuilder(SampleRecord(record)).build_location()

    assert result["name"] is None
    assert result["geo"] == {
        "@type": "GeoCoordinates",
        "latitude": "43.5 DD",
        "longitude": "7.8 DD",
        "elevation": "10 m",
    }


def test_build_location_empty():
    record = make_record()
    result = ActionBuilder(SampleRecord(record)).build_location()

    assert result["name"] is None
    assert result["geo"] is None
    assert result["additionalProperty"] is None


def test_build_instrument_single_value():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={
                "@type": "DefinedTerm",
                "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
            },
        )
    )
    result = ActionBuilder(SampleRecord(record)).build_instrument()
    assert result == [
        {
            "@type": "Product",
            "description": "sample collection device",
            "name": "CTD rosette",
            "category": "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
        }
    ]


def test_build_instrument_multi_value():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette|Niskin bottle",
            value_reference=[
                {
                    "@type": "DefinedTerm",
                    "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
                },
                {
                    "@type": "DefinedTerm",
                    "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0412/",
                },
            ],
        )
    )
    result = ActionBuilder(SampleRecord(record)).build_instrument()
    assert result == [
        {
            "@type": "Product",
            "description": "sample collection device",
            "name": ["CTD rosette", "Niskin bottle"],
            "category": [
                "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
                "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0412/",
            ],
        }
    ]


# ---------------------------------------------------------------------------
# Provenance: source URLs and per-record sidecars
# ---------------------------------------------------------------------------


def test_sample_source_urls():
    assert sample_source_urls("SAMEA1") == [
        "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.ldjson",
        "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.json",
    ]


@pytest.fixture
def offline_biosamples_input(tmp_path, monkeypatch):
    """An input dir holding an Excel file the glob finds, with sample-id discovery
    and the per-sample network fetch stubbed so fetch_biosamples runs offline for a
    single sample SAMEA1."""
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    (input_dir / "samples.xlsx").touch()
    monkeypatch.setattr(
        "metadata_converter.biosamples.run.get_sample_ids",
        lambda excel_file, cfg: {"SAMEA1"},
    )
    monkeypatch.setattr(
        "metadata_converter.biosamples.run.fetch_sample",
        lambda sid, path, cfg: True,
    )
    return input_dir


def test_biosamples_fetch_writes_provenance(tmp_path, offline_biosamples_input):
    config = BiosamplesConfig(
        input=BiosamplesInput(input_dir=offline_biosamples_input),
        output=FetchedOutputConfig(
            input=tmp_path / "fetched", loaded_base=tmp_path / "loaded_base"
        ),
        provenance_dir=tmp_path / "provenance",
    )

    fetch_biosamples(config)

    doc = json.loads(
        (tmp_path / "provenance" / "Provenance_SAMEA1.jsonld").read_text()
    )
    assert doc == {
        "@context": {"@vocab": "https://schema.org/"},
        "@type": "DigitalDocument",
        "@id": "Provenance_SAMEA1.jsonld",
        "about": {"@type": "Thing", "@id": "SAMEA1.jsonld"},
        "isBasedOn": [
            {"@type": "CreativeWork", "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.ldjson"},
            {"@type": "CreativeWork", "@id": "https://www.ebi.ac.uk/biosamples/samples/SAMEA1.json"},
        ],
        "description": "stage: load",
        "dateCreated": doc["dateCreated"],
    }
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", doc["dateCreated"])


def test_biosamples_fetch_without_provenance_dir_writes_nothing(
    tmp_path, offline_biosamples_input
):
    config = BiosamplesConfig(
        input=BiosamplesInput(input_dir=offline_biosamples_input),
        output=FetchedOutputConfig(
            input=tmp_path / "fetched", loaded_base=tmp_path / "loaded_base"
        ),
        provenance_dir=None,
    )

    fetch_biosamples(config)

    assert not (tmp_path / "provenance").exists()
