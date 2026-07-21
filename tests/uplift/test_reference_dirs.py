"""Tests for GenericUpliftConfig.reference_dirs.

Entities loaded from a reference directory (e.g. an already-uplifted sibling
source, such as biosamples' Products that datahub links against) must be
available to LinkApplier as match candidates, but — unlike input_dir — must
never be written to output_dir or provenance_dir.
"""
from metadata_converter.uplift import run_uplift
from tests.uplift.conftest import load_jsonld, write_jsonld


def test_reference_dir_candidates_are_linked(config_factory, tmp_path):
    reference_dir = tmp_path / "reference"
    write_jsonld(reference_dir / "Product_SAMEA0002.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Product", "@id": "Product_SAMEA0002.jsonld",
        "identifier": "SAMEA0002",
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "sample:analysis-pid", "value": "analysis-pid-1"},
        ],
    })
    cfg = config_factory(reference_dirs=reference_dir, out_name="ref_linked")

    run_uplift(cfg)

    action = load_jsonld(cfg.output_dir / "Action_analysis1.jsonld")
    # sort by @id: candidate order across stores is not part of the contract
    assert sorted(action["object"], key=lambda ref: ref["@id"]) == [
        {"@type": "Product", "@id": "Product_SAMEA0001.jsonld"},
        {"@type": "Product", "@id": "Product_SAMEA0002.jsonld"},
    ]
    # siblings untouched by the link assignment
    assert action["name"] == "Analysis 1"
    assert action["identifier"] == "analysis-pid-1"


def test_reference_dir_entities_are_not_written_to_output(config_factory, tmp_path):
    reference_dir = tmp_path / "reference"
    write_jsonld(reference_dir / "Product_SAMEA0002.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Product", "@id": "Product_SAMEA0002.jsonld",
        "identifier": "SAMEA0002",
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "sample:analysis-pid", "value": "analysis-pid-1"},
        ],
    })
    cfg = config_factory(reference_dirs=reference_dir, out_name="ref_not_copied")

    run_uplift(cfg)

    written = {p.name for p in cfg.output_dir.glob("*.jsonld")}
    assert written == {
        "Person_alice.jsonld", "Person_bob.jsonld", "DataCatalog_main.jsonld",
        "Action_analysis1.jsonld", "Product_SAMEA0001.jsonld", "Dataset_file1.jsonld",
    }


def test_reference_dir_entities_get_no_provenance_file(config_factory, tmp_path):
    reference_dir = tmp_path / "reference"
    write_jsonld(reference_dir / "Product_SAMEA0002.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Product", "@id": "Product_SAMEA0002.jsonld",
        "identifier": "SAMEA0002",
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "sample:analysis-pid", "value": "analysis-pid-1"},
        ],
    })
    cfg = config_factory(
        reference_dirs=reference_dir,
        provenance_dir=tmp_path / "provenance",
        out_name="ref_no_provenance",
    )

    run_uplift(cfg)

    written = {p.name for p in cfg.provenance_dir.glob("*.jsonld")}
    assert written == {
        "Provenance_uplift_Person_alice.jsonld",
        "Provenance_uplift_Person_bob.jsonld",
        "Provenance_uplift_DataCatalog_main.jsonld",
        "Provenance_uplift_Action_analysis1.jsonld",
        "Provenance_uplift_Product_SAMEA0001.jsonld",
        "Provenance_uplift_Dataset_file1.jsonld",
    }