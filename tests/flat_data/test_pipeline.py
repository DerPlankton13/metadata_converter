"""Integration tests for the flat_data pipeline.

Each subdirectory of ``data/`` is one scenario. A scenario contains
``config.toml`` (load) with its ``input/`` CSVs and expected ``loaded_base/``,
and one or more uplift variants, each a subdirectory ``uplift_*/`` holding its
own ``uplift.toml`` and expected ``loaded_uplifted/`` and ``provenance/``.

Every uplift variant of a scenario consumes the shared ``loaded_base/`` as its
input — this keeps the two stage tests independent, so a single bug fails one
test rather than cascading through both, and lets variants differ in one setting
(e.g. ``atomize``) only.

The merge stage is tested on the ``atomize_true`` variant only: atomizing is what
leaves the stub nodes the merge has to collapse.
"""

import json
import re
from pathlib import Path

import pandas as pd
import pytest
from rdflib import SDO, Graph, URIRef

from metadata_converter.config import load_source_config, load_uplift_config
from metadata_converter.flat_data.run import load_flat_data
from metadata_converter.uplift import run_uplift
from metadata_converter.uplift.merge import merge_entities_by_identifier
from metadata_converter.utils.graph_helpers import load_graph

DATA_DIR = Path(__file__).parent / "data"
LOAD_SCENARIOS = [
    pytest.param(DATA_DIR / "minimal", id="minimal"),
    pytest.param(DATA_DIR / "full", id="full"),
]
UPLIFT_SCENARIOS = [
    pytest.param(DATA_DIR / "full" / "uplift_atomize_false", id="full-atomize_false"),
    pytest.param(DATA_DIR / "full" / "uplift_atomize_true", id="full-atomize_true"),
]
MERGE_INPUT_DIR = DATA_DIR / "full" / "uplift_atomize_true" / "loaded_uplifted"
BASE = "https://example.org/"


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
    """Compare the ``stage`` provenance files a run wrote (``produced_dir``) against the
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


@pytest.mark.parametrize("scenario_dir", LOAD_SCENARIOS)
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


@pytest.mark.parametrize("scenario_dir", UPLIFT_SCENARIOS)
def test_uplift_produces_expected_jsonld_corpus(scenario_dir, tmp_path):
    output_dir = tmp_path / "loaded_uplifted"

    cfg = load_uplift_config(str(scenario_dir / "uplift.toml"))
    cfg.input_dir = scenario_dir / cfg.input_dir
    cfg.output_dir = output_dir
    cfg.provenance_dir = tmp_path / "provenance"

    run_uplift(cfg)

    assert_jsonld_dir_matches(output_dir, scenario_dir / "loaded_uplifted")
    assert_provenance_matches(
        tmp_path / "provenance", scenario_dir / "provenance", "uplift"
    )


def write_corpus_as_turtle(corpus_dir: Path, target: Path) -> None:
    """Build the graph file the way the paper's ``graph-parser`` does (Oxigraph store, ``ox-json-ld``)."""
    g = Graph(store="Oxigraph")
    g.bind("local", BASE)
    for file in corpus_dir.glob("**/*.jsonld"):
        g.parse(file, format="ox-json-ld", publicID=BASE)
    g.serialize(destination=str(target), format="ox-ttl")


def count_nodes_by_type(g: Graph) -> dict[str, int]:
    """Count distinct nodes per rdf:type, keyed by the type's local name."""
    rows = g.query(
        "SELECT ?t (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s a ?t } GROUP BY ?t"
    )
    return {str(t).rsplit("/", 1)[-1]: int(n) for t, n in rows}


def test_merge_atomize_true_collapses_node_count_per_type(tmp_path):
    graph_in = tmp_path / "graph.ttl"
    graph_out = tmp_path / "merged.ttl"
    write_corpus_as_turtle(MERGE_INPUT_DIR, graph_in)

    merge_entities_by_identifier(graph_in, graph_out, tmp_path / "provenance")

    assert count_nodes_by_type(load_graph(str(graph_in))) == {
        "Action": 2,
        "DataCatalog": 1,
        "Dataset": 9,
        "MonetaryGrant": 1,
        "Organization": 3,
        "Person": 5,
        "Product": 2,
        "Project": 1,
        "PropertyValue": 3,
    }
    assert count_nodes_by_type(load_graph(str(graph_out))) == {
        "Action": 2,
        "DataCatalog": 1,
        "Dataset": 5,
        "MonetaryGrant": 1,
        "Organization": 3,
        "Person": 3,
        "Product": 2,
        "Project": 1,
        "PropertyValue": 3,
    }


def test_merge_atomize_true_leaves_full_entities_and_repoints_references(tmp_path):
    graph_in = tmp_path / "graph.ttl"
    graph_out = tmp_path / "merged.ttl"
    write_corpus_as_turtle(MERGE_INPUT_DIR, graph_in)
    ada = URIRef(f"{BASE}Person_eO5j5Fw4jJgY77YD5B2Bp3.jsonld")
    peter = URIRef(f"{BASE}Person_K5wlCRfqw97XI27QWkbJlM.jsonld")
    file1 = URIRef(f"{BASE}Dataset_ADiahyGvC3qOUpY075lWIo.jsonld")
    file2 = URIRef(f"{BASE}Dataset_wUWOL1dIkAujGkgr4E5iij.jsonld")
    file3 = URIRef(f"{BASE}Dataset_54mFck-PrXwxW6E3MbsdRU.jsonld")
    file5 = URIRef(f"{BASE}Dataset_mJqkxqEjVs1OTCehLjpAd0.jsonld")
    metabarcoding = URIRef(f"{BASE}Action_B41mGtUFQp3FTskkVZ1k8Q.jsonld")
    imaging = URIRef(f"{BASE}Action_ZwCMF-8WhKFk2jZwKUcadl.jsonld")
    stubs = {
        URIRef(f"{BASE}Person_BWLDqVYY0_zLlw5-hYjtfj.jsonld"),
        URIRef(f"{BASE}Person_NspCdQy0qq9T-nsmen-cpj.jsonld"),
        URIRef(f"{BASE}Dataset_jxhehwyxtkZ-b8rP2KFm5s.jsonld"),
        URIRef(f"{BASE}Dataset_mCButIZMGmVf1StKg6yokj.jsonld"),
        URIRef(f"{BASE}Dataset_c3gAcOuviu_c2vKchp2qWi.jsonld"),
        URIRef(f"{BASE}Dataset_3V2jG7cu1LhN-bUoklT10v.jsonld"),
    }
    before = load_graph(str(graph_in))

    merge_entities_by_identifier(graph_in, graph_out, tmp_path / "provenance")
    after = load_graph(str(graph_out))

    # the stubs donated nothing, so every full entity keeps exactly its own triples
    for full_entity in (ada, peter, file1, file2, file3, file5):
        assert set(after.predicate_objects(full_entity)) == set(
            before.predicate_objects(full_entity)
        )
    # references that pointed at a stub now point at the full entity
    assert set(after.objects(metabarcoding, SDO.agent)) == {ada, peter}
    assert set(after.objects(metabarcoding, SDO.result)) == {file1, file5, file3}
    assert set(after.objects(imaging, SDO.agent)) == {ada}
    assert set(after.objects(imaging, SDO.result)) == {file5, file2}
    # no stub remains, neither as a node with its own triples nor as a reference target
    node_ids = set(after.subjects())
    referenced_ids = set(after.objects())
    assert stubs & node_ids == set()
    assert stubs & referenced_ids == set()
