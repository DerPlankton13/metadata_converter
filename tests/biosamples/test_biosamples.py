import json
from pathlib import Path

import pytest
from deepdiff import DeepDiff

from metadata_converter.biosamples.fetch import fuse_metadata
from metadata_converter.biosamples.uplifting import SampleUplifter, build_defined_term

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
