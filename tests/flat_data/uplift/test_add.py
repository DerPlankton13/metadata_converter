"""Tests for ``AddApplier`` in ``uplift/add.py`` and the AdditionRule config.

An addition sets ``target_property`` on every entity of ``on_type`` to a fixed
constant. The constant is either a *literal* (a scalar DataType value) or a *node*
(a typed mapping that builds a schema.org object, recursively). Models are built in
non-strict mode; an unknown field is kept but logged as a warning, while an unknown
``type`` cannot be built and raises. An addition overwrites any existing value and
logs when it does.
"""
import logging

import pytest

from metadata_converter.flat_data.uplift.add import AddApplier
from metadata_converter.flat_data.uplift.config import (
    AdditionRule,
    FlatDataUpliftConfig,
    LinkRule,
)
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.schemaorg_models import Person, Project

# ---------------------------------------------------------------------------
# AddApplier.apply — setting literal and node values
# ---------------------------------------------------------------------------


def test_add_literal_property():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    AddApplier(store).apply(
        AdditionRule(
            on_type="Person", target_property="jobTitle", value="Researcher"
        )
    )

    [person] = store.of_type("Person")
    assert person.jobTitle == "Researcher"
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"


def test_add_node_property():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    AddApplier(store).apply(
        AdditionRule(
            on_type="Person",
            target_property="memberOf",
            value={"type": "Project", "id": "https://example.com/project.jsonld"},
        )
    )

    [person] = store.of_type("Person")
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"
    assert isinstance(person.memberOf, Project)
    assert person.memberOf.id == "https://example.com/project.jsonld"
    assert person.memberOf.type == "Project"


def test_add_node_list():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    AddApplier(store).apply(
        AdditionRule(
            on_type="Person",
            target_property="memberOf",
            value=[
                {"type": "Project", "id": "https://example.com/p1.jsonld"},
                {"type": "Project", "id": "https://example.com/p2.jsonld"},
            ],
        )
    )

    [person] = store.of_type("Person")
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"
    assert len(person.memberOf) == 2
    assert isinstance(person.memberOf[0], Project)
    assert person.memberOf[0].id == "https://example.com/p1.jsonld"
    assert isinstance(person.memberOf[1], Project)
    assert person.memberOf[1].id == "https://example.com/p2.jsonld"


def test_add_node_without_type_raises():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    with pytest.raises(ValueError, match="must carry a 'type' key"):
        AddApplier(store).apply(
            AdditionRule(
                on_type="Person",
                target_property="memberOf",
                value={"id": "https://example.com/x.jsonld"},
            )
        )


def test_add_overwrites_existing_value(caplog):
    store = EntityStore()
    store.by_type["Person"] = [
        Person(
            id="Person_alice.jsonld",
            name="Alice",
            memberOf=Project(id="https://example.com/project_old.jsonld"),
        )
    ]

    with caplog.at_level(logging.WARNING):
        AddApplier(store).apply(
            AdditionRule(
                on_type="Person",
                target_property="memberOf",
                value={"type": "Project", "id": "https://example.com/project_new.jsonld"},
            )
        )

    [person] = store.of_type("Person")
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"
    assert person.memberOf.id == "https://example.com/project_new.jsonld"
    assert "overwrit" in caplog.text.lower()


def test_add_applies_to_all_entities():
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", name="Alice"),
        Person(id="Person_bob.jsonld", name="Bob"),
    ]

    AddApplier(store).apply(
        AdditionRule(
            on_type="Person", target_property="jobTitle", value="Researcher"
        )
    )

    alice, bob = store.of_type("Person")
    assert alice.jobTitle == "Researcher"
    assert alice.id == "Person_alice.jsonld"
    assert alice.name == "Alice"
    assert bob.jobTitle == "Researcher"
    assert bob.id == "Person_bob.jsonld"
    assert bob.name == "Bob"


def test_add_apply_all_applies_each_rule():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    AddApplier(store).apply_all([
        AdditionRule(on_type="Person", target_property="jobTitle", value="Researcher"),
        AdditionRule(on_type="Person", target_property="description", value="A scientist"),
    ])

    [person] = store.of_type("Person")
    assert person.jobTitle == "Researcher"
    assert person.description == "A scientist"
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"


def test_add_unknown_type_raises():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    with pytest.raises(ValueError, match="unknown class name 'Nonexistent'"):
        AddApplier(store).apply(
            AdditionRule(
                on_type="Person",
                target_property="memberOf",
                value={"type": "Nonexistent", "id": "https://example.com/x.jsonld"},
            )
        )


def test_add_unknown_field_warns(caplog):
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    with caplog.at_level(logging.WARNING):
        AddApplier(store).apply(
            AdditionRule(
                on_type="Person",
                target_property="memberOf",
                value={
                    "type": "Project",
                    "id": "https://example.com/project.jsonld",
                    "notarealfield": "x",
                },
            )
        )

    [person] = store.of_type("Person")
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"
    assert person.memberOf.id == "https://example.com/project.jsonld"
    assert person.memberOf.notarealfield == "x"
    assert "notarealfield" in caplog.text


# ---------------------------------------------------------------------------
# Overlap validator — additions participate
# ---------------------------------------------------------------------------


def test_link_and_add_on_same_target_raises(tmp_path):
    with pytest.raises(ValueError, match="Person.memberOf is targeted by multiple"):
        FlatDataUpliftConfig(
            input_dir=tmp_path / "in",
            output_dir=tmp_path / "out",
            links=[
                LinkRule(
                    on_type="Person",
                    target_property="memberOf",
                    match_literal="1",
                    in_type="Project",
                    in_property="identifier",
                ),
            ],
            additions=[
                AdditionRule(
                    on_type="Person",
                    target_property="memberOf",
                    value={"type": "Project", "id": "https://example.com/project.jsonld"},
                ),
            ],
        )
