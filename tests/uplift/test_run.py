"""Tests for ``run_uplift``'s provenance side-effect: each uplifted entity gets a
provenance file recording the loaded entity (by @id) it was based on. Since uplift
refines an entity in place, isBasedOn's @id equals about's @id — the stage
distinguishes them."""
import json

from metadata_converter.uplift import run_uplift
from metadata_converter.uplift.config import (
    GenericUpliftConfig,
    RemovalRule,
    RemovalWhere,
)
from tests.uplift.conftest import load_jsonld, write_jsonld


def test_uplift_writes_provenance(config_factory, tmp_path):
    cfg = config_factory(provenance_dir=tmp_path / "provenance")

    run_uplift(cfg)

    provenance_files = list((tmp_path / "provenance").glob("*.jsonld"))
    assert len(provenance_files) == 6

    alice = json.loads(
        (tmp_path / "provenance" / "Provenance_uplift_Person_alice.jsonld").read_text()
    )
    assert alice["about"] == {"@type": "Thing", "@id": "Person_alice.jsonld"}
    assert alice["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "Person_alice.jsonld",
    }
    assert alice["description"] == "stage: uplift"

    # A second, distinct entity must point at its own @id, not alice's —
    # guards against a constant isBasedOn/about written for every entity.
    bob = json.loads(
        (tmp_path / "provenance" / "Provenance_uplift_Person_bob.jsonld").read_text()
    )
    assert bob["about"] == {"@type": "Thing", "@id": "Person_bob.jsonld"}
    assert bob["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "Person_bob.jsonld",
    }
    assert bob["description"] == "stage: uplift"


def test_uplift_without_provenance_dir_writes_nothing(config_factory, tmp_path):
    cfg = config_factory(provenance_dir=None)

    run_uplift(cfg)

    assert not (tmp_path / "provenance").exists()


# ---------------------------------------------------------------------------
# atomize wiring: config flag, ordering relative to write/provenance/removal
# ---------------------------------------------------------------------------


def test_uplift_atomize_default_true_extracts_blank_nodes(loaded_base, tmp_path):
    cfg = GenericUpliftConfig(
        input_dir=loaded_base, output_dir=tmp_path / "out", links=[]
    )

    run_uplift(cfg)

    action = load_jsonld(cfg.output_dir / "Action_analysis1.jsonld")
    assert action["agent"] == {
        "@type": "Person", "@id": "Person_Njtdsk7B9jXU3438K9WYXX.jsonld"
    }
    atom = load_jsonld(cfg.output_dir / "Person_Njtdsk7B9jXU3438K9WYXX.jsonld")
    assert atom["identifier"] == "0000-0002-2222-2222"


def test_uplift_atomize_false_leaves_blank_nodes_embedded(config_factory, tmp_path):
    cfg = config_factory(out_name="no_atomize", rules=[], atomize=False)

    run_uplift(cfg)

    action = load_jsonld(cfg.output_dir / "Action_analysis1.jsonld")
    assert action["agent"] == {"@type": "Person", "identifier": "0000-0002-2222-2222"}
    assert not (cfg.output_dir / "Person_Njtdsk7B9jXU3438K9WYXX.jsonld").exists()


def test_uplift_atomize_extracted_entity_gets_provenance_naming_its_origin(
        loaded_base, tmp_path
):
    cfg = GenericUpliftConfig(
        input_dir=loaded_base,
        output_dir=tmp_path / "out",
        provenance_dir=tmp_path / "provenance",
        links=[],
        atomize=True,
    )

    run_uplift(cfg)

    atom_id = "Person_Njtdsk7B9jXU3438K9WYXX.jsonld"
    assert (cfg.output_dir / atom_id).exists()
    # an atom has no single origin, so it records the entity it was extracted from
    doc = load_jsonld(cfg.provenance_dir / f"Provenance_uplift_{atom_id}")
    assert doc["about"] == {"@type": "Thing", "@id": atom_id}
    assert doc["isBasedOn"] == {
        "@type": "CreativeWork",
        "@id": "Action_analysis1.jsonld",
    }
    # the original, now-mutated top-level entity still gets its own provenance file
    assert (cfg.provenance_dir / "Provenance_uplift_Action_analysis1.jsonld").exists()


def test_uplift_atomize_shared_atom_records_every_origin(tmp_path):
    """One atom reached from two entities lists both — this is what makes the
    provenance of a cross-source deduplicated atom correct once the graph merges it."""
    input_dir = tmp_path / "in"
    for id in ("Action_one.jsonld", "Action_two.jsonld"):
        write_jsonld(input_dir / id, {
            "@context": {"@vocab": "https://schema.org"},
            "@type": "Action", "@id": id,
            # identical blank node in both -> one shared atom
            "agent": {"@type": "Person", "identifier": "0000-0002-2222-2222"},
        })
    cfg = GenericUpliftConfig(
        input_dir=input_dir,
        output_dir=tmp_path / "out",
        provenance_dir=tmp_path / "provenance",
        atomize=True,
    )

    run_uplift(cfg)

    atoms = sorted(p.name for p in cfg.output_dir.glob("Person_*.jsonld"))
    assert len(atoms) == 1, atoms
    doc = load_jsonld(cfg.provenance_dir / f"Provenance_uplift_{atoms[0]}")
    assert sorted(ref["@id"] for ref in doc["isBasedOn"]) == [
        "Action_one.jsonld",
        "Action_two.jsonld",
    ]


def test_uplift_atomize_runs_after_removal_scrubbed_content_not_atomized(tmp_path):
    input_dir = tmp_path / "in"
    write_jsonld(input_dir / "Person_x.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Person", "@id": "Person_x.jsonld",
        "name": "X",
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "scaffold", "value": "1"},
        ],
    })
    cfg = GenericUpliftConfig(
        input_dir=input_dir,
        output_dir=tmp_path / "out",
        removals=[
            RemovalRule(
                on_type="Person", target_property="additionalProperty",
                where=RemovalWhere(property="name", equals="scaffold"),
            )
        ],
        atomize=True,
    )

    run_uplift(cfg)

    written = {p.name for p in cfg.output_dir.glob("*.jsonld")}
    assert written == {"Person_x.jsonld"}
