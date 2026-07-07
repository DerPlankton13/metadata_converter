"""Shared fixtures for the flat_data uplift test suite.

The ``loaded_base`` fixture writes a minimal datahub-shaped JSON-LD corpus into
``tmp_path / "loaded_base"``. ``config_factory`` builds a ``FlatDataUpliftConfig``
against it with optional rule/drop overrides. ``uplifted`` runs the engine and
returns the loaded output files keyed by filename.
"""
import json
from pathlib import Path

import pytest

from metadata_converter.flat_data.uplift import run_uplift
from metadata_converter.flat_data.uplift.config import FlatDataUpliftConfig, LinkRule


# Module-level constant: the 5 datahub-style link rules used across many tests.
# Covers forward lookup, reverse lookup (via additionalProperty), and literal matching.
DATAHUB_RULES: list[LinkRule] = [
    LinkRule(
        on_type="Action", target_property="agent",
        match_value="agent.identifier",
        in_type="Person", in_property="identifier",
    ),
    LinkRule(
        on_type="Action", target_property="object",
        match_value="identifier",
        in_type="Product",
        in_additional_property="sample:analysis-pid",
    ),
    LinkRule(
        on_type="Action", target_property="result",
        match_value="identifier",
        in_type="Dataset",
        in_additional_property="file:analysis",
    ),
    LinkRule(
        on_type="DataCatalog", target_property="creator",
        match_literal="1",
        in_type="Person",
        in_additional_property="author:is-dataset-author",
    ),
    LinkRule(
        on_type="Dataset", target_property="about",
        match_value="about.identifier",
        in_type="Product", in_property="identifier",
    ),
]


def write_jsonld(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def load_jsonld(path: Path) -> dict:
    return json.loads(path.read_text())


@pytest.fixture
def loaded_base(tmp_path) -> Path:
    """A minimal datahub-shaped loaded corpus.

    Tests may mutate files in this directory before invoking the engine.
    """
    d = tmp_path / "loaded_base"
    write_jsonld(d / "Person_alice.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Person", "@id": "Person_alice.jsonld",
        "name": "Alice Author",
        "identifier": {"@type": "Orcid", "value": "0000-0001-1111-1111"},
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "author:is-dataset-author", "value": 1},
        ],
    })
    write_jsonld(d / "Person_bob.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Person", "@id": "Person_bob.jsonld",
        "name": "Bob Builder",
        "identifier": {"@type": "Orcid", "value": "0000-0002-2222-2222"},
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "author:is-dataset-author", "value": 0},
        ],
    })
    write_jsonld(d / "DataCatalog_main.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "DataCatalog", "@id": "DataCatalog_main.jsonld",
        "name": "BIOcean5D Test Catalog", "identifier": "dataset-pid-1",
    })
    write_jsonld(d / "Action_analysis1.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Action", "@id": "Action_analysis1.jsonld",
        "name": "Analysis 1", "identifier": "analysis-pid-1",
        "agent": {"@type": "Person", "identifier": "0000-0002-2222-2222"},
    })
    write_jsonld(d / "Product_SAMEA0001.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Product", "@id": "Product_SAMEA0001.jsonld",
        "identifier": "SAMEA0001",
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "sample:analysis-pid", "value": "analysis-pid-1"},
        ],
    })
    write_jsonld(d / "Dataset_file1.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Dataset", "@id": "Dataset_file1.jsonld",
        "name": "file1.csv", "url": "https://example.org/file1.csv",
        "about": {"@type": "Product", "identifier": "SAMEA0001"},
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "file:analysis", "value": "analysis-pid-1"},
        ],
    })
    return d


@pytest.fixture
def config_factory(loaded_base, tmp_path):
    """Returns a callable that builds a FlatDataUpliftConfig with optional overrides."""
    def make(
        *,
        rules: list[LinkRule] | None = None,
        out_name: str = "uplifted",
        provenance_dir: Path | None = None,
    ) -> FlatDataUpliftConfig:
        return FlatDataUpliftConfig(
            input_dir=loaded_base,
            output_dir=tmp_path / out_name,
            links=rules if rules is not None else DATAHUB_RULES,
            provenance_dir=provenance_dir,
        )
    return make


@pytest.fixture
def uplifted(config_factory) -> dict[str, dict]:
    """Run the default datahub uplift and return ``{filename → loaded dict}``."""
    cfg = config_factory()
    run_uplift(cfg)
    return {p.name: load_jsonld(p) for p in cfg.output_dir.glob("*.jsonld")}
