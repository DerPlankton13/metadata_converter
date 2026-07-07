"""Tests for ``LinkApplier`` in ``uplift/link.py``: rule application and edge cases."""
import logging

import pytest

from metadata_converter.uplift import run_uplift
from metadata_converter.uplift.config import LinkRule
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.uplift.link import LinkApplier
from metadata_converter.schema_org_models.schemaorg_models import Action, Person, Product
from tests.uplift.conftest import load_jsonld, write_jsonld


# ---------------------------------------------------------------------------
# Rule resolution: each link rule produces the expected reference shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, target_property, expected_ref",
    [
        pytest.param(
            "Action_analysis1.jsonld", "agent",
            {"@type": "Person", "@id": "Person_bob.jsonld"},
            id="forward-agent",
        ),
        pytest.param(
            "Action_analysis1.jsonld", "object",
            {"@type": "Product", "@id": "Product_SAMEA0001.jsonld"},
            id="reverse-object",
        ),
        pytest.param(
            "Action_analysis1.jsonld", "result",
            {"@type": "Dataset", "@id": "Dataset_file1.jsonld"},
            id="reverse-result",
        ),
        pytest.param(
            "DataCatalog_main.jsonld", "creator",
            {"@type": "Person", "@id": "Person_alice.jsonld"},
            id="flag-creator",
        ),
        pytest.param(
            "Dataset_file1.jsonld", "about",
            {"@type": "Product", "@id": "Product_SAMEA0001.jsonld"},
            id="forward-about",
        ),
    ],
)
def test_link_resolves_to_reference(uplifted, filename, target_property, expected_ref):
    assert uplifted[filename][target_property] == expected_ref


# ---------------------------------------------------------------------------
# Edge cases — engine resilience
# ---------------------------------------------------------------------------


def test_unresolvable_agent_orcid_keeps_stub_intact(loaded_base, config_factory):
    action = load_jsonld(loaded_base / "Action_analysis1.jsonld")
    action["agent"]["identifier"] = "0000-0009-9999-9999"  # nobody has this
    write_jsonld(loaded_base / "Action_analysis1.jsonld", action)
    cfg = config_factory(out_name="unresolvable")
    run_uplift(cfg)
    agent = load_jsonld(cfg.output_dir / "Action_analysis1.jsonld")["agent"]
    assert agent.get("identifier") == "0000-0009-9999-9999"
    assert agent.get("@id") is None


def test_multiple_samples_for_one_analysis_aggregate_into_list(loaded_base, config_factory):
    write_jsonld(loaded_base / "Product_SAMEA0002.jsonld", {
        "@context": {"@vocab": "https://schema.org"},
        "@type": "Product", "@id": "Product_SAMEA0002.jsonld",
        "identifier": "SAMEA0002",
        "additionalProperty": [
            {"@type": "PropertyValue",
             "name": "sample:analysis-pid", "value": "analysis-pid-1"},
        ],
    })
    cfg = config_factory(out_name="multi_sample")
    run_uplift(cfg)
    action = load_jsonld(cfg.output_dir / "Action_analysis1.jsonld")
    assert action["object"] == [
        {"@type": "Product", "@id": "Product_SAMEA0001.jsonld"},
        {"@type": "Product", "@id": "Product_SAMEA0002.jsonld"},
    ]


def test_ref_id_template_constructs_id_from_candidate_property(loaded_base, config_factory):
    # Use ref_id_template so the ref @id is built from the sample's identifier
    # rather than taken directly from the stub's @id.
    rule = LinkRule(
        on_type="Dataset", target_property="about",
        match_value="about.identifier",
        in_type="Product", in_property="identifier",
        ref_id_template="Product_{identifier}.jsonld",
    )
    cfg = config_factory(rules=[rule], out_name="ref_id_template")
    run_uplift(cfg)
    about = load_jsonld(cfg.output_dir / "Dataset_file1.jsonld")["about"]
    assert about == {"@type": "Product", "@id": "Product_SAMEA0001.jsonld"}


# ---------------------------------------------------------------------------
# Literal matching semantics — int vs bool are intentionally distinct.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("flag_value", [1, "1"])
def test_literal_1_matches_int_and_string_one(loaded_base, config_factory, flag_value):
    alice = load_jsonld(loaded_base / "Person_alice.jsonld")
    alice["additionalProperty"][0]["value"] = flag_value
    write_jsonld(loaded_base / "Person_alice.jsonld", alice)
    cfg = config_factory(out_name=f"literal_1_{flag_value!r}")
    run_uplift(cfg)
    assert load_jsonld(cfg.output_dir / "DataCatalog_main.jsonld")["creator"] == {
        "@type": "Person", "@id": "Person_alice.jsonld",
    }


@pytest.mark.parametrize("flag_value", [True, "true"])
def test_literal_true_matches_bool_and_string(loaded_base, config_factory, flag_value):
    alice = load_jsonld(loaded_base / "Person_alice.jsonld")
    alice["additionalProperty"][0]["value"] = flag_value
    write_jsonld(loaded_base / "Person_alice.jsonld", alice)
    rule = LinkRule(
        on_type="DataCatalog", target_property="creator",
        match_literal="true",
        in_type="Person", in_additional_property="author:is-dataset-author",
    )
    cfg = config_factory(rules=[rule], out_name=f"literal_true_{flag_value!r}")
    run_uplift(cfg)
    assert load_jsonld(cfg.output_dir / "DataCatalog_main.jsonld")["creator"] == {
        "@type": "Person", "@id": "Person_alice.jsonld",
    }


# ---------------------------------------------------------------------------
# Error and edge paths — direct LinkApplier unit tests
# ---------------------------------------------------------------------------


def test_link_unknown_in_type_skips_rule(caplog):
    store = EntityStore()
    store.by_type["Action"] = [Action(id="Action_1.jsonld", identifier="p1")]

    with caplog.at_level(logging.WARNING):
        LinkApplier(store).apply(LinkRule(
            on_type="Action", target_property="object",
            match_value="identifier", in_type="Nonexistent", in_property="identifier",
        ))


    [action] = store.of_type("Action")
    assert action.object is None
    assert "unknown @type" in caplog.text


def test_link_no_candidates_leaves_target_unset(caplog):
    store = EntityStore()
    store.by_type["Action"] = [Action(id="Action_1.jsonld", identifier="p1")]

    with caplog.at_level(logging.WARNING):
        LinkApplier(store).apply(LinkRule(
            on_type="Action", target_property="object",
            match_value="identifier", in_type="Product", in_property="identifier",
        ))

    [action] = store.of_type("Action")
    assert action.object is None
    assert "no candidates of @type 'Product'" in caplog.text


def test_link_missing_match_value_skips_entity():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_1.jsonld", identifier="p1")]
    store.by_type["Action"] = [Action(id="Action_1.jsonld")]

    LinkApplier(store).apply(LinkRule(
        on_type="Action", target_property="agent",
        match_value="agent.identifier", in_type="Person", in_property="identifier",
    ))

    [action] = store.of_type("Action")
    assert action.agent is None


def test_link_unrenderable_template_skips_match(caplog):
    store = EntityStore()
    store.by_type["Product"] = [Product(id="Product_1.jsonld", identifier="p1")]
    store.by_type["Action"] = [Action(id="Action_1.jsonld", identifier="p1")]

    with caplog.at_level(logging.WARNING):
        LinkApplier(store).apply(LinkRule(
            on_type="Action", target_property="object",
            match_value="identifier", in_type="Product", in_property="identifier",
            ref_id_template="Product_{missing}.jsonld",
        ))

    [action] = store.of_type("Action")
    assert action.object is None
    assert "could not be rendered" in caplog.text


def test_link_invalid_assignment_skips_entity(caplog):
    store = EntityStore()
    store.by_type["Product"] = [Product(id="Product_1.jsonld", identifier="p1")]
    store.by_type["Person"] = [Person(id="Person_1.jsonld", identifier="p1")]

    with caplog.at_level(logging.WARNING):
        LinkApplier(store).apply(LinkRule(
            on_type="Person", target_property="birthDate",
            match_value="identifier", in_type="Product", in_property="identifier",
        ))

    [person] = store.of_type("Person")
    assert person.birthDate is None
    assert "assignment failed" in caplog.text


@pytest.mark.parametrize("flag_value", [0, False])
def test_falsy_flag_values_leave_creator_unset(loaded_base, config_factory, flag_value):
    alice = load_jsonld(loaded_base / "Person_alice.jsonld")
    alice["additionalProperty"][0]["value"] = flag_value
    write_jsonld(loaded_base / "Person_alice.jsonld", alice)
    bob = load_jsonld(loaded_base / "Person_bob.jsonld")
    bob["additionalProperty"][0]["value"] = flag_value
    write_jsonld(loaded_base / "Person_bob.jsonld", bob)
    cfg = config_factory(out_name=f"falsy_{flag_value!r}")
    run_uplift(cfg)
    assert "creator" not in load_jsonld(cfg.output_dir / "DataCatalog_main.jsonld")
