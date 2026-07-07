"""Tests for ``RenameApplier`` in ``uplift/rename.py`` and the RenameRule config.

A rename moves the value held at ``source_property`` to ``target_property`` on every
entity of ``on_type``, then clears ``source_property``. When the source has no value,
nothing changes. A rename overwrites any existing value at ``target_property`` and logs
when it does, matching ``AddApplier``'s overwrite behavior.
"""
import logging

import pytest

from metadata_converter.config import (
    AdditionRule,
    FlatDataUpliftConfig,
    RenameRule,
)
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.flat_data.uplift.rename import RenameApplier
from metadata_converter.schema_org_models.schemaorg_models import Person

# ---------------------------------------------------------------------------
# RenameApplier.apply — moving values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("job_title", ["Researcher", None])
def test_rename_moves_value_or_leaves_target_unset(job_title):
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", name="Alice", jobTitle=job_title)
    ]

    RenameApplier(store).apply(
        RenameRule(on_type="Person", source_property="jobTitle", target_property="description")
    )

    [person] = store.of_type("Person")
    assert person.description == job_title
    assert person.jobTitle is None
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"


def test_rename_overwrites_existing_target(caplog):
    store = EntityStore()
    store.by_type["Person"] = [
        Person(
            id="Person_alice.jsonld",
            name="Alice",
            jobTitle="Researcher",
            description="Old",
        )
    ]

    with caplog.at_level(logging.WARNING):
        RenameApplier(store).apply(
            RenameRule(on_type="Person", source_property="jobTitle", target_property="description")
        )

    [person] = store.of_type("Person")
    assert person.description == "Researcher"
    assert person.jobTitle is None
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"
    assert "overwrit" in caplog.text.lower()


def test_rename_applies_to_all_entities():
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", name="Alice", jobTitle="Researcher"),
        Person(id="Person_bob.jsonld", name="Bob", jobTitle="Engineer"),
    ]

    RenameApplier(store).apply(
        RenameRule(on_type="Person", source_property="jobTitle", target_property="description")
    )

    alice, bob = store.of_type("Person")
    assert alice.description == "Researcher"
    assert alice.jobTitle is None
    assert alice.id == "Person_alice.jsonld"
    assert alice.name == "Alice"
    assert bob.description == "Engineer"
    assert bob.jobTitle is None
    assert bob.id == "Person_bob.jsonld"
    assert bob.name == "Bob"


def test_rename_apply_all_applies_each_rule():
    store = EntityStore()
    store.by_type["Person"] = [
        Person(
            id="Person_alice.jsonld",
            name="Alice",
            jobTitle="Researcher",
            additionalName="Al",
        )
    ]

    RenameApplier(store).apply_all([
        RenameRule(on_type="Person", source_property="jobTitle", target_property="description"),
        RenameRule(on_type="Person", source_property="additionalName", target_property="alternateName"),
    ])

    [person] = store.of_type("Person")
    assert person.description == "Researcher"
    assert person.jobTitle is None
    assert person.alternateName == "Al"
    assert person.additionalName is None
    assert person.id == "Person_alice.jsonld"
    assert person.name == "Alice"


# ---------------------------------------------------------------------------
# Overlap validator — renames are exempt
# ---------------------------------------------------------------------------


def test_rename_and_addition_on_same_target_does_not_raise(tmp_path):
    FlatDataUpliftConfig(
        input_dir=tmp_path / "in",
        output_dir=tmp_path / "out",
        additions=[
            AdditionRule(
                on_type="Person",
                target_property="description",
                value="A scientist",
            ),
        ],
        renames=[
            RenameRule(
                on_type="Person",
                source_property="jobTitle",
                target_property="description",
            ),
        ],
    )
