import json
import shutil

import pytest

from metadata_converter.biosamples.config import BiosamplesUpliftConfig
from metadata_converter.biosamples.run import uplift_biosamples
from metadata_converter.biosamples.uplifting import SampleUplifter
from tests.biosamples.conftest import DATA_DIR, assert_no_diff, load_json, strip_none

SAMPLE_IDS = ["SAMEA112489011", "SAMEA111477556", "SAMEA118673980"]


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_extract_product(sample_id):
    expected = load_json(DATA_DIR / f"Product_{sample_id}.jsonld")
    data = load_json(DATA_DIR / f"CreativeWork_{sample_id}.jsonld")

    product, _ = SampleUplifter(data).build_dicts()
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected), strip_none(product))


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_extract_action(sample_id):
    expected = load_json(DATA_DIR / f"Action_{sample_id}.jsonld")
    data = load_json(DATA_DIR / f"CreativeWork_{sample_id}.jsonld")

    _, action = SampleUplifter(data).build_dicts()
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected), strip_none(action))


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_uplift_biosamples_writes_expected_product_and_action(tmp_path, sample_id):
    expected_product = load_json(DATA_DIR / f"Product_{sample_id}.jsonld")
    expected_action = load_json(DATA_DIR / f"Action_{sample_id}.jsonld")
    input_dir = tmp_path / "loaded"
    input_dir.mkdir()
    shutil.copy(DATA_DIR / f"CreativeWork_{sample_id}.jsonld", input_dir)
    config = BiosamplesUpliftConfig(
        source_type="biosamples",
        input_dir=input_dir,
        output_dir=tmp_path / "uplifted",
        provenance_dir=None,
    )

    uplift_biosamples(config)

    result_product = load_json(tmp_path / "uplifted" / f"Product_{sample_id}.jsonld")
    result_action = load_json(tmp_path / "uplifted" / f"Action_{sample_id}.jsonld")
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected_product), strip_none(result_product))
    assert_no_diff(strip_none(expected_action), strip_none(result_action))


def test_biosamples_uplift_writes_provenance(tmp_path, loaded_sample):
    input_dir, sid = loaded_sample
    config = BiosamplesUpliftConfig(
        source_type="biosamples",
        input_dir=input_dir,
        output_dir=tmp_path / "uplifted",
        provenance_dir=tmp_path / "provenance",
    )

    uplift_biosamples(config)

    product_doc = json.loads(
        (
            tmp_path / "provenance" / f"Provenance_uplift_record_Product_{sid}.jsonld"
        ).read_text()
    )
    assert product_doc["about"] == {"@type": "Thing", "@id": f"Product_{sid}.jsonld"}
    assert product_doc["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": f"CreativeWork_{sid}.jsonld",
    }
    assert product_doc["description"] == "stage: uplift_record"

    action_doc = json.loads(
        (tmp_path / "provenance" / f"Provenance_uplift_record_Action_{sid}.jsonld").read_text()
    )
    assert action_doc["about"] == {"@type": "Thing", "@id": f"Action_{sid}.jsonld"}
    assert action_doc["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": f"CreativeWork_{sid}.jsonld",
    }
    assert action_doc["description"] == "stage: uplift_record"


def test_biosamples_uplift_without_provenance_dir_writes_nothing(
    tmp_path, loaded_sample
):
    input_dir, _ = loaded_sample
    config = BiosamplesUpliftConfig(
        source_type="biosamples",
        input_dir=input_dir,
        output_dir=tmp_path / "uplifted",
        provenance_dir=None,
    )

    uplift_biosamples(config)

    assert not (tmp_path / "provenance").exists()
