"""Tests for the pure ``atomize_blank_nodes`` transform in ``uplift/atomize.py``."""

import pytest

from metadata_converter.schema_org_models.schemaorg_models import (
    CreativeWork,
    Organization,
    Person,
)
from metadata_converter.uplift.atomize import AtomizeApplier, atomize_blank_nodes
from metadata_converter.uplift.entity_store import EntityStore


def test_atomize_blank_nodes_no_nested_schema_objects_returns_unchanged_and_empty_list():
    entity = CreativeWork(id="CreativeWork_1.jsonld", name="Sample dataset")

    result, atomized = atomize_blank_nodes(entity)

    assert result.id == "CreativeWork_1.jsonld"
    assert result.name == "Sample dataset"
    assert atomized == []


def test_atomize_blank_nodes_scalar_blank_node_extracted_and_replaced_with_ref():
    entity = CreativeWork(
        id="CreativeWork_1.jsonld",
        name="Sample dataset",
        author=Person(name="Jane Doe"),
    )

    result, atomized = atomize_blank_nodes(entity)

    assert result.author == Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld")
    assert result.name == "Sample dataset"
    assert result.id == "CreativeWork_1.jsonld"
    assert atomized == [
        Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld", name="Jane Doe")
    ]


def test_atomize_blank_nodes_nested_blank_node_extracted_bottom_up():
    entity = CreativeWork(
        id="CreativeWork_1.jsonld",
        author=Person(
            name="Jane Doe", worksFor=Organization(name="Acme Corp")
        ),
    )

    result, atomized = atomize_blank_nodes(entity)

    assert result.author == Person(id="Person_ZZiYTZ7DiKcIXTbaEZDzgG.jsonld")
    assert atomized == [
        Organization(id="Organization_sROsvZTN0HaB6VlkrkDO3l.jsonld", name="Acme Corp"),
        Person(
            id="Person_ZZiYTZ7DiKcIXTbaEZDzgG.jsonld",
            name="Jane Doe",
            worksFor=Organization(id="Organization_sROsvZTN0HaB6VlkrkDO3l.jsonld"),
        ),
    ]


def test_atomize_blank_nodes_already_identified_nested_node_left_untouched():
    entity = CreativeWork(
        id="CreativeWork_1.jsonld",
        author=Person(id="Person_existing.jsonld", name="Jane Doe"),
    )

    result, atomized = atomize_blank_nodes(entity)

    assert result.author == Person(id="Person_existing.jsonld", name="Jane Doe")
    assert atomized == []


def test_atomize_blank_nodes_list_valued_property_mixed_blank_and_identified_items():
    entity = CreativeWork(
        id="CreativeWork_1.jsonld",
        creator=[
            Person(name="Jane Doe"),
            Organization(id="Organization_existing.jsonld", name="Acme"),
        ],
    )

    result, atomized = atomize_blank_nodes(entity)

    assert result.creator == [
        Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld"),
        Organization(id="Organization_existing.jsonld", name="Acme"),
    ]
    assert atomized == [
        Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld", name="Jane Doe")
    ]


def test_atomize_blank_nodes_duplicate_blank_content_across_properties_dedupes_to_one_entry():
    entity = CreativeWork(
        id="CreativeWork_1.jsonld",
        author=Person(name="Jane Doe"),
        creator=Person(name="Jane Doe"),
    )

    result, atomized = atomize_blank_nodes(entity)

    assert result.author.id == "Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld"
    assert result.creator.id == "Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld"
    assert atomized == [
        Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld", name="Jane Doe")
    ]


def test_atomize_blank_nodes_blank_node_under_extra_property_replaced_with_ref():
    entity = CreativeWork(
        id="CreativeWork_1.jsonld",
        **{"notModeledProperty": Person(name="Jane Doe")},
    )

    result, atomized = atomize_blank_nodes(entity)

    assert result.model_extra["notModeledProperty"] == Person(
        id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld"
    )
    assert atomized == [
        Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld", name="Jane Doe")
    ]


def test_atomize_blank_nodes_context_field_does_not_affect_hash():
    entity_without_nested_context = CreativeWork(
        id="CreativeWork_1.jsonld",
        context="https://schema.org/",
        author=Person(name="Jane Doe"),
    )
    entity_with_nested_context = CreativeWork(
        id="CreativeWork_1.jsonld",
        context="https://schema.org/",
        author=Person(context="https://schema.org/", name="Jane Doe"),
    )

    result_without_nested_context, _ = atomize_blank_nodes(entity_without_nested_context)
    result_with_nested_context, _ = atomize_blank_nodes(entity_with_nested_context)

    assert result_without_nested_context.author.id == result_with_nested_context.author.id


def test_atomize_blank_nodes_does_not_mutate_input_entity():
    entity = CreativeWork(
        id="CreativeWork_1.jsonld",
        author=Person(name="Jane Doe"),
    )

    atomize_blank_nodes(entity)

    assert entity.author.id is None
    assert entity.author.name == "Jane Doe"


# ---------------------------------------------------------------------------
# AtomizeApplier.apply — wiring atomize_blank_nodes into the EntityStore
# ---------------------------------------------------------------------------


def test_atomize_applier_no_blank_nodes_leaves_store_unchanged():
    store = EntityStore()
    store.by_type["CreativeWork"] = [
        CreativeWork(id="CreativeWork_1.jsonld", name="Sample dataset")
    ]
    store.by_type["Person"] = [Person(id="Person_existing.jsonld", name="Jane Doe")]

    AtomizeApplier(store).apply()

    assert store.of_type("CreativeWork") == [
        CreativeWork(id="CreativeWork_1.jsonld", name="Sample dataset")
    ]
    assert store.of_type("Person") == [
        Person(id="Person_existing.jsonld", name="Jane Doe")
    ]
    assert store.by_type.keys() == {"CreativeWork", "Person"}


def test_atomize_applier_blank_node_extracted_into_new_type_bucket():
    store = EntityStore()
    store.by_type["CreativeWork"] = [
        CreativeWork(id="CreativeWork_1.jsonld", author=Person(name="Jane Doe"))
    ]

    AtomizeApplier(store).apply()

    [dataset] = store.of_type("CreativeWork")
    assert dataset.id == "CreativeWork_1.jsonld"
    assert dataset.author == Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld")
    assert store.of_type("Person") == [
        Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld", name="Jane Doe")
    ]


def test_atomize_applier_dedupes_identical_blank_content_across_entities():
    store = EntityStore()
    store.by_type["CreativeWork"] = [
        CreativeWork(id="CreativeWork_1.jsonld", author=Person(name="Jane Doe")),
        CreativeWork(id="CreativeWork_2.jsonld", author=Person(name="Jane Doe")),
    ]

    AtomizeApplier(store).apply()

    [first, second] = store.of_type("CreativeWork")
    assert first.id == "CreativeWork_1.jsonld"
    assert second.id == "CreativeWork_2.jsonld"
    assert first.author.id == "Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld"
    assert second.author.id == "Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld"
    assert store.of_type("Person") == [
        Person(id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld", name="Jane Doe")
    ]


def test_atomize_applier_id_collision_with_different_content_raises():
    store = EntityStore()
    store.by_type["Person"] = [
        Person(
            id="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld", name="Already Loaded Version"
        )
    ]
    store.by_type["CreativeWork"] = [
        CreativeWork(id="CreativeWork_1.jsonld", author=Person(name="Jane Doe"))
    ]

    with pytest.raises(ValueError, match="Person_9UdOq2MiGRJzHZ3NpYI4a-.jsonld"):
        AtomizeApplier(store).apply()
