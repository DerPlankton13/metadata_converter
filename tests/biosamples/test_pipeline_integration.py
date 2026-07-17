"""Full-chain fetch -> load -> uplift integration test for the biosamples pipeline."""

import shutil

import pytest

from metadata_converter.biosamples.config import BiosamplesUpliftConfig
from metadata_converter.biosamples.run import load_biosamples, uplift_biosamples
from tests.biosamples.conftest import (
    DATA_DIR,
    assert_no_diff,
    biosamples_config,
    load_json,
    strip_none,
)

SAMPLE_IDS = ["SAMEA112489011", "SAMEA111477556", "SAMEA118673980"]


@pytest.mark.parametrize("sample_id", SAMPLE_IDS)
def test_fetch_to_uplift_produces_expected_product_and_action(tmp_path, sample_id):
    expected_product = load_json(DATA_DIR / f"Product_{sample_id}.jsonld")
    expected_action = load_json(DATA_DIR / f"Action_{sample_id}.jsonld")

    fetched_dir = tmp_path / "fetched"
    fetched_dir.mkdir()
    shutil.copy(DATA_DIR / f"{sample_id}.ldjson", fetched_dir)
    shutil.copy(DATA_DIR / f"{sample_id}.json", fetched_dir)
    loaded_dir = tmp_path / "loaded"
    uplifted_dir = tmp_path / "uplifted"

    load_biosamples(biosamples_config(fetched_dir, loaded_dir, None))
    uplift_biosamples(
        BiosamplesUpliftConfig(
            source_type="biosamples",
            input_dir=loaded_dir,
            output_dir=uplifted_dir,
            provenance_dir=None,
        )
    )

    result_product = load_json(uplifted_dir / f"Product_{sample_id}.jsonld")
    result_action = load_json(uplifted_dir / f"Action_{sample_id}.jsonld")
    # I consider the dicts the be equal, even if they contain additional None entries
    assert_no_diff(strip_none(expected_product), strip_none(result_product))
    assert_no_diff(strip_none(expected_action), strip_none(result_action))
