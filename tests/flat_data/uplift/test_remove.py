"""Tests for ``RemoveApplier`` in ``uplift/remove.py`` and the RemovalRule config.

Removal filters items out of a list-valued property by a predicate on each item.
The predicate reads a (possibly nested) subproperty via the shared selector,
compares on string form, and is case-sensitive. Matching items are removed; an
emptied list collapses to ``None``.
"""
import pytest
from metadata_converter.flat_data.uplift.remove import RemoveApplier

from metadata_converter.config import (
    EnrichmentRule,
    FlatDataUpliftConfig,
    RemovalRule,
    RemovalWhere,
)
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.schemaorg_models import (
    Dataset,
    DefinedTerm,
    PropertyValue,
)

# ---------------------------------------------------------------------------
# RemoveApplier.apply — filtering list items by predicate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "where",
    [
        pytest.param(
            RemovalWhere(property="name", equals="file:analysis"), id="equals"
        ),
        pytest.param(
            RemovalWhere(property="description", contains="Helper Property"),
            id="contains",
        ),
    ],
)
def test_removal_filters_matching_item(where):
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(
                    name="file:analysis",
                    description="Helper Property for linking",
                    value="metabarcoding",
                ),
                PropertyValue(
                    name="keep-me",
                    description="A real description",
                    value="real data",
                ),
                PropertyValue(
                    name="keep-me-too",
                    description="Another real description",
                    value="more data",
                ),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset", target_property="additionalProperty", where=where
        )
    )

    [dataset] = store.of_type("Dataset")
    assert len(dataset.additionalProperty) == 2
    assert dataset.additionalProperty[0].name == "keep-me"
    assert dataset.additionalProperty[0].value == "real data"
    assert dataset.additionalProperty[1].name == "keep-me-too"
    assert dataset.additionalProperty[1].value == "more data"


def test_removal_removes_all_matching_items():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(name="file:analysis", value="metabarcoding"),
                PropertyValue(name="file:analysis", value="imaging"),
                PropertyValue(name="keep-me", value="real data"),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="name", equals="file:analysis"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty.name == "keep-me"
    assert dataset.additionalProperty.value == "real data"


def test_removal_collapses_to_single_survivor():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(name="file:analysis", value="metabarcoding"),
                PropertyValue(name="keep-me", value="real data"),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="name", equals="file:analysis"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty.name == "keep-me"
    assert dataset.additionalProperty.value == "real data"


def test_removal_collapses_emptied_list_to_none():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(name="file:analysis", value="metabarcoding"),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="name", equals="file:analysis"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty is None


def test_removal_matches_nested_subproperty():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(
                    name="env",
                    valueReference=DefinedTerm(termCode="ENVO_001"),
                ),
                PropertyValue(
                    name="other",
                    valueReference=DefinedTerm(termCode="NCBI_999"),
                ),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="valueReference.termCode", equals="ENVO_001"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert len(dataset.additionalProperty) == 1
    assert dataset.additionalProperty[0].name == "other"
    assert dataset.additionalProperty[0].valueReference.termCode == "NCBI_999"


def test_removal_keeps_item_when_predicate_field_absent():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(
                    name="file:analysis", description="Helper Property for linking"
                ),
                PropertyValue(name="keep-me"),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="description", contains="Helper Property for"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert len(dataset.additionalProperty) == 1
    assert dataset.additionalProperty[0].name == "keep-me"


def test_removal_matches_when_any_resolved_value_matches():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(
                    name="env",
                    valueReference=[
                        DefinedTerm(termCode="ENVO_001"),
                        DefinedTerm(termCode="NCBI_999"),
                    ],
                ),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="valueReference.termCode", equals="ENVO_001"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty is None


def test_removal_matching_single_item():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=PropertyValue(name="file:analysis", value="metabarcoding"),
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="name", equals="file:analysis"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty is None


def test_removal_keeps_non_matching_single_item():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=PropertyValue(name="keep-me", value="real data"),
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="name", equals="file:analysis"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty.name == "keep-me"
    assert dataset.additionalProperty.value == "real data"


def test_removal_match_is_case_sensitive():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(name="File:Analysis", value="metabarcoding"),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="name", equals="file:analysis"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert len(dataset.additionalProperty) == 1
    assert dataset.additionalProperty[0].name == "File:Analysis"
    assert dataset.additionalProperty[0].value == "metabarcoding"


def test_removal_matches_on_string_form():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_f1.jsonld",
            additionalProperty=[
                PropertyValue(name="flag", value=1),
            ],
        )
    ]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="value", equals="1"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty is None


def test_removal_skips_missing_property():
    store = EntityStore()
    store.by_type["Dataset"] = [Dataset(id="Dataset_f1.jsonld", name="no extras")]

    RemoveApplier(store).apply(
        RemovalRule(
            on_type="Dataset",
            target_property="additionalProperty",
            where=RemovalWhere(property="name", equals="file:analysis"),
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.additionalProperty is None


# ---------------------------------------------------------------------------
# RemovalWhere config validation
# ---------------------------------------------------------------------------


def test_where_with_both_modes_raises():
    with pytest.raises(ValueError, match="exactly one of 'equals' or 'contains'"):
        RemovalWhere(property="name", equals="x", contains="y")


def test_where_with_no_match_mode_raises():
    with pytest.raises(ValueError, match="exactly one of 'equals' or 'contains'"):
        RemovalWhere(property="name")


# ---------------------------------------------------------------------------
# Overlap validator — removals are exempt
# ---------------------------------------------------------------------------


def test_removal_may_overlap_other_rules(tmp_path):
    # A removal targeting the same (on_type, target_property) as an enrichment
    # must NOT trip the overlap validator — removals legitimately undo/refine.
    cfg = FlatDataUpliftConfig(
        input_dir=tmp_path / "in",
        output_dir=tmp_path / "out",
        enrichments=[
            EnrichmentRule(
                on_type="Person", target_property="identifier", enrich_as="Orcid"
            ),
        ],
        removals=[
            RemovalRule(
                on_type="Person",
                target_property="identifier",
                where=RemovalWhere(property="name", equals="scaffold"),
            ),
        ],
    )
    assert len(cfg.removals) == 1
