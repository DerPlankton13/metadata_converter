import json
import shutil
from pathlib import Path

import pytest
from deepdiff import DeepDiff

from metadata_converter.biosamples.config import (
    BiosamplesConfig,
    BiosamplesExtractorConfig,
)

DATA_DIR = Path(__file__).parent / "data"


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


def biosamples_config(fetched_dir, output_dir, provenance_dir):
    """A minimal BiosamplesConfig for the load step."""
    return BiosamplesConfig(
        extractor=BiosamplesExtractorConfig(input=fetched_dir),
        fetched_dir=fetched_dir,
        output_dir=output_dir,
        provenance_dir=provenance_dir,
    )


def write_fetched_sample(fetched_dir, sample_id):
    """A minimal fetched .ldjson/.json pair for `sample_id`, fusable without error."""
    structured = {
        "@id": f"biosample:{sample_id}",
        "@type": "DataRecord",
        "@context": [
            "http://schema.org",
            {"biosample": "http://identifiers.org/biosample/"},
        ],
        "mainEntity": {"@type": ["Sample", "OBI:0000747"], "additionalProperty": []},
    }
    (fetched_dir / f"{sample_id}.ldjson").write_text(json.dumps(structured))
    (fetched_dir / f"{sample_id}.json").write_text(json.dumps({"characteristics": {}}))


@pytest.fixture
def loaded_sample(tmp_path):
    """An isolated input dir holding one loaded sample, returned with its sample id."""
    sample_id = "SAMEA111477556"
    input_dir = tmp_path / "loaded_base"
    input_dir.mkdir()
    shutil.copy(DATA_DIR / f"CreativeWork_{sample_id}.jsonld", input_dir)
    return input_dir, sample_id
