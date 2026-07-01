"""Integration tests for the flat_data pipeline.

Each subdirectory of ``data/`` is one scenario. A scenario contains
``config.toml`` (load), ``uplift.toml`` (uplift), or both. Inputs live under
``input/`` (CSVs for load, JSON-LD for uplift-only scenarios). Expected
outputs live under ``loaded_base/`` and ``loaded_uplifted/``.

For full-pipeline scenarios (both TOMLs present), the uplift test consumes
``loaded_base/`` as its input — this keeps the two stage tests
independent, so a single bug fails one test rather than cascading through both.
"""

import json
import re
from pathlib import Path

import pandas as pd
import pytest

from metadata_converter.config import load_source_config, load_uplift_config
from metadata_converter.flat_data.run import load_flat_data
from metadata_converter.flat_data.uplift import run_uplift

DATA_DIR = Path(__file__).parent / "data"
LOAD_SCENARIOS = sorted(p.parent for p in DATA_DIR.glob("*/config.toml"))
UPLIFT_SCENARIOS = sorted(p.parent for p in DATA_DIR.glob("*/uplift.toml"))


def build_xlsx(input_dir: Path, target: Path) -> Path:
    """Assemble the per-sheet CSVs in ``input_dir`` into a single xlsx file."""
    with pd.ExcelWriter(target) as writer:
        for csv in sorted(input_dir.glob("*.csv")):
            pd.read_csv(csv).to_excel(writer, sheet_name=csv.stem, index=False)
    return target


def assert_jsonld_dir_matches(produced_dir: Path, golden_dir: Path) -> None:
    produced = {p.name: json.loads(p.read_text()) for p in produced_dir.glob("*.jsonld")}
    golden = {
        p.name: json.loads(p.read_text()) for p in golden_dir.glob("*.jsonld")
    }
    assert set(produced) == set(golden)
    for filename in golden:
        assert produced[filename] == golden[filename], f"mismatch in {filename}"


def comparable_provenance(provenance: dict) -> dict:
    """Return the form of a provenance doc that's safe to compare against a golden
    file: the two non-reproducible fields are neutralised — ``dateCreated`` (wall-clock)
    is dropped, and ``isBasedOn`` (an absolute source path) is reduced to its basename,
    the portable part that identifies the file."""
    provenance = dict(provenance)  # shallow copy so we don't mutate the caller's dict
    provenance.pop("dateCreated", None)
    based = provenance["isBasedOn"]
    if isinstance(based, list):
        provenance["isBasedOn"] = [{**b, "@id": b["@id"].split("/")[-1]} for b in based]
    else:
        provenance["isBasedOn"] = {**based, "@id": based["@id"].split("/")[-1]}
    return provenance


def assert_provenance_matches(produced_dir: Path, golden_dir: Path, stage: str) -> None:
    """Compare the ``stage`` sidecars a run wrote (``produced_dir``) against the
    committed golden corpus (``golden_dir``).

    ``dateCreated`` is only checked for ISO-8601 shape (its value is non-reproducible);
    every other field, including ``isBasedOn`` by basename, must match the golden file.
    """
    produced = {p.name: json.loads(p.read_text()) for p in produced_dir.glob("*.jsonld")}
    golden = {
        p.name: json.loads(p.read_text())
        for p in golden_dir.glob(f"Provenance_{stage}_*.jsonld")
    }
    assert set(produced) == set(golden)
    for filename in golden:
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", produced[filename]["dateCreated"]
        ), f"bad dateCreated in {filename}"
        assert comparable_provenance(produced[filename]) == comparable_provenance(
            golden[filename]
        ), f"mismatch in {filename}"


@pytest.mark.parametrize("scenario_dir", LOAD_SCENARIOS, ids=lambda p: p.name)
def test_load_produces_expected_jsonld_corpus(scenario_dir, tmp_path):
    xlsx = build_xlsx(scenario_dir / "input", tmp_path / "input.xlsx")
    output_dir = tmp_path / "loaded_base"

    cfg = load_source_config(str(scenario_dir / "config.toml"))
    cfg.extractor.input = xlsx
    cfg.output_dir = output_dir
    cfg.provenance_dir = tmp_path / "provenance"

    load_flat_data(cfg)

    assert_jsonld_dir_matches(output_dir, scenario_dir / "loaded_base")
    assert_provenance_matches(
        tmp_path / "provenance", scenario_dir / "provenance", "load"
    )


@pytest.mark.parametrize("scenario_dir", UPLIFT_SCENARIOS, ids=lambda p: p.name)
def test_uplift_produces_expected_jsonld_corpus(scenario_dir, tmp_path):
    output_dir = tmp_path / "loaded_uplifted"

    cfg = load_uplift_config(str(scenario_dir / "uplift.toml")).flat_data
    cfg.input_dir = [scenario_dir / d for d in cfg.input_dir]
    cfg.output_dir = output_dir
    cfg.provenance_dir = tmp_path / "provenance"

    run_uplift(cfg)

    assert_jsonld_dir_matches(output_dir, scenario_dir / "loaded_uplifted")
    assert_provenance_matches(
        tmp_path / "provenance", scenario_dir / "provenance", "uplift"
    )
