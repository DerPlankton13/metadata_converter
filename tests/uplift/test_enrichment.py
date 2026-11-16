"""Tests for ``EnrichmentApplier`` in ``uplift/enrichment.py`` and the overlap validator.

Enrichment wraps a scalar property value in a project-defined PropertyValue
subclass (Orcid, DOI, …). The class's Pydantic validators auto-populate the
metadata fields (url, name, propertyID, etc.).
"""

import logging

import pytest

from metadata_converter.uplift.config import (
    AdditionRule,
    Condition,
    EnrichmentRule,
    GenericUpliftConfig,
)
from metadata_converter.uplift.enrichment import EnrichmentApplier
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.custom_models import DOI, ISBN, ISSN, Orcid
from metadata_converter.schema_org_models.schemaorg_models import (
    Dataset,
    Person,
    PropertyValue,
)

VALID_ORCID_A = "0000-0001-1111-1111"
VALID_ORCID_B = "0000-0002-2222-2222"


# ---------------------------------------------------------------------------
# EnrichmentApplier.apply
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "identifier_value",
    [
        pytest.param(VALID_ORCID_A, id="scalar"),
        pytest.param([VALID_ORCID_A], id="singleton-list"),
    ],
)
def test_enrichment_wraps_scalar_into_orcid_with_full_metadata(identifier_value):
    """An Orcid enrichment populates @type, value, url, name, alternateName, and propertyID
    from the bare scalar — the auto-enrichment is the visible payoff of this feature.

    A singleton list collapses to its element first, then takes the same path as a
    bare scalar — both inputs produce the same Orcid PropertyValue.
    """
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", identifier=identifier_value)
    ]

    EnrichmentApplier(store).apply(
        EnrichmentRule(
            on_type="Person", target_property="identifier", enrich_as="Orcid"
        )
    )

    [person] = store.of_type("Person")
    assert isinstance(person.identifier, Orcid)
    assert person.identifier.type == "PropertyValue"
    assert person.identifier.value == VALID_ORCID_A
    assert str(person.identifier.url) == f"https://orcid.org/{VALID_ORCID_A}"
    assert person.identifier.alternateName == "ORCID"
    assert person.identifier.name == "Open Researcher and Contributor ID"
    assert (
        str(person.identifier.propertyID)
        == "https://registry.identifiers.org/registry/orcid"
    )


def test_enrichment_skips_already_wrapped_value():
    prewrapped = Orcid(value=VALID_ORCID_A)
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", identifier=prewrapped)]

    EnrichmentApplier(store).apply(
        EnrichmentRule(
            on_type="Person", target_property="identifier", enrich_as="Orcid"
        )
    )

    [person] = store.of_type("Person")
    assert person.identifier is prewrapped


def test_enrichment_plain_property_value_becomes_doi_with_clean_value():
    """The scalar inside a plain PropertyValue is wrapped, not the model's repr, which
    would leave a stray quote on the DOI value and url."""
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_a.jsonld",
            name="Survey",
            identifier=PropertyValue(value="10.1234/abc.def"),
        )
    ]

    EnrichmentApplier(store).apply(
        EnrichmentRule(
            on_type="Dataset", target_property="identifier", enrich_as="DOI"
        )
    )

    [dataset] = store.of_type("Dataset")
    assert isinstance(dataset.identifier, DOI)
    assert dataset.identifier.type == "PropertyValue"
    assert dataset.identifier.value == "10.1234/abc.def"
    assert str(dataset.identifier.url) == "https://doi.org/10.1234/abc.def"
    assert dataset.identifier.alternateName == "DOI"
    assert dataset.identifier.name == "Digital Object Identifier"
    assert (
        str(dataset.identifier.propertyID)
        == "https://registry.identifiers.org/registry/doi"
    )
    assert dataset.id == "Dataset_a.jsonld"
    assert dataset.name == "Survey"


def test_enrichment_plain_property_value_with_invalid_value_keeps_original(caplog):
    original = PropertyValue(value="not a doi")
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(id="Dataset_a.jsonld", name="Survey", identifier=original)
    ]

    EnrichmentApplier(store).apply(
        EnrichmentRule(
            on_type="Dataset", target_property="identifier", enrich_as="DOI"
        )
    )

    [dataset] = store.of_type("Dataset")
    assert dataset.identifier is original
    assert dataset.id == "Dataset_a.jsonld"
    assert dataset.name == "Survey"
    assert "Invalid DOI: not a doi" in caplog.text


def test_enrichment_skips_missing_property():
    store = EntityStore()
    store.by_type["Person"] = [Person(id="Person_alice.jsonld", name="Alice")]

    EnrichmentApplier(store).apply(
        EnrichmentRule(
            on_type="Person", target_property="identifier", enrich_as="Orcid"
        )
    )

    [person] = store.of_type("Person")
    assert person.identifier is None


def test_enrichment_raises_when_value_has_multiple_entries():
    """An entity should not carry multiple identifiers of the same type — a
    list of more than one entry is treated as a likely data error rather than
    silently fanned out.
    """
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", identifier=[VALID_ORCID_A, VALID_ORCID_B])
    ]

    with pytest.raises(ValueError, match="list of identifiers of the same type"):
        EnrichmentApplier(store).apply(
            EnrichmentRule(
                on_type="Person", target_property="identifier", enrich_as="Orcid"
            )
        )


def test_enrichment_with_unknown_class_name_raises():
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", identifier=VALID_ORCID_A)
    ]

    with pytest.raises(ValueError, match="unknown class name"):
        EnrichmentApplier(store).apply(
            EnrichmentRule(
                on_type="Person", target_property="identifier", enrich_as="Nonexistent"
            )
        )


def test_enrichment_with_non_property_value_class_raises():
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", identifier=VALID_ORCID_A)
    ]

    with pytest.raises(ValueError, match="must name a PropertyValue subclass"):
        EnrichmentApplier(store).apply(
            EnrichmentRule(
                on_type="Person", target_property="identifier", enrich_as="Person"
            )
        )


# ---------------------------------------------------------------------------
# Overlap validator on GenericUpliftConfig
# ---------------------------------------------------------------------------


def test_two_enrichments_on_same_target_raise_at_config_load(tmp_path):
    with pytest.raises(
        ValueError, match=r"Person\.identifier.*targeted by multiple uplift rules"
    ):
        GenericUpliftConfig(
            input_dir=tmp_path / "in",
            output_dir=tmp_path / "out",
            provenance_dir=tmp_path / "provenance",
            enrichments=[
                EnrichmentRule(
                    on_type="Person", target_property="identifier", enrich_as="Orcid"
                ),
                EnrichmentRule(
                    on_type="Person", target_property="identifier", enrich_as="DOI"
                ),
            ],
        )


# ---------------------------------------------------------------------------
# Conditional enrichment
# ---------------------------------------------------------------------------

DOI_CONDITION = Condition(if_property="propertyID", equals="DOI")


def doi_rule(condition=DOI_CONDITION):
    return EnrichmentRule(
        on_type="Dataset",
        target_property="identifier",
        enrich_as="DOI",
        condition=condition,
    )


def test_conditional_enrichment_wraps_only_values_meeting_the_condition(caplog):
    caplog.set_level(logging.DEBUG)
    labelled_doi = PropertyValue(value="10.1234/abc.def", propertyID="DOI")
    labelled_uri = PropertyValue(value="10.1234/xyz", propertyID="URI")
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(id="Dataset_a.jsonld", identifier=labelled_doi),
        Dataset(id="Dataset_b.jsonld", identifier=labelled_uri),
    ]

    EnrichmentApplier(store).apply(doi_rule())

    a, b = store.of_type("Dataset")
    assert isinstance(a.identifier, DOI)
    assert b.identifier is labelled_uri
    assert "Dataset_b.jsonld has propertyID='URI', condition needs 'DOI'" in caplog.text
    assert "wrapped 1 (skipped 0 with no value, 0 already wrapped, 1 not meeting" in (
        caplog.text
    )


def test_conditional_enrichment_never_matches_a_plain_string():
    store = EntityStore()
    store.by_type["Person"] = [
        Person(id="Person_alice.jsonld", identifier=VALID_ORCID_A)
    ]

    EnrichmentApplier(store).apply(
        EnrichmentRule(
            on_type="Person",
            target_property="identifier",
            enrich_as="Orcid",
            condition=Condition(if_property="propertyID", equals="ORCID"),
        )
    )

    [person] = store.of_type("Person")
    assert person.identifier == VALID_ORCID_A


def test_conditional_rules_on_one_property_each_wrap_their_own_values():
    doi = PropertyValue(value="10.1234/abc.def", propertyID="DOI")
    issn = PropertyValue(value="1234-5678", propertyID="ISSN")
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(id="Dataset_a.jsonld", identifier=doi),
        Dataset(id="Dataset_b.jsonld", identifier=issn),
    ]

    EnrichmentApplier(store).apply_all(
        [
            doi_rule(),
            EnrichmentRule(
                on_type="Dataset",
                target_property="identifier",
                enrich_as="ISSN",
                condition=Condition(if_property="propertyID", equals="ISSN"),
            ),
        ]
    )

    a, b = store.of_type("Dataset")
    assert a.identifier.alternateName == "DOI"
    assert b.identifier.alternateName == "ISSN"


def test_condition_requires_both_keys_and_rejects_unknown_ones():
    with pytest.raises(ValueError, match="equals"):
        Condition(if_property="propertyID")
    with pytest.raises(ValueError, match="extra"):
        Condition(if_property="propertyID", equals="DOI", value="DOI")


# ---------------------------------------------------------------------------
# Overlap validator with conditions
# ---------------------------------------------------------------------------


def make_config(tmp_path, enrichments=(), additions=()):
    return GenericUpliftConfig(
        input_dir=tmp_path / "in",
        output_dir=tmp_path / "out",
        provenance_dir=tmp_path / "provenance",
        enrichments=list(enrichments),
        additions=list(additions),
    )


def test_conditions_on_same_property_with_different_values_are_allowed(tmp_path):
    make_config(
        tmp_path,
        enrichments=[
            doi_rule(),
            doi_rule(Condition(if_property="propertyID", equals="ISSN")),
        ],
    )


@pytest.mark.parametrize(
    "other",
    [
        pytest.param(doi_rule(), id="identical-condition"),
        pytest.param(doi_rule(None), id="unconditional-enrichment"),
        pytest.param(
            doi_rule(Condition(if_property="url", equals="x")),
            id="condition-on-different-property",
        ),
    ],
)
def test_conditional_rule_overlapping_another_raises(tmp_path, other):
    for rules in ([doi_rule(), other], [other, doi_rule()]):
        with pytest.raises(ValueError, match="targeted by multiple uplift rules"):
            make_config(tmp_path, enrichments=rules)


def test_conditional_enrichment_and_addition_on_same_target_raise(tmp_path):
    with pytest.raises(ValueError, match="targeted by multiple uplift rules"):
        make_config(
            tmp_path,
            enrichments=[doi_rule()],
            additions=[
                AdditionRule(
                    on_type="Dataset", target_property="identifier", value="fixed"
                )
            ],
        )


def test_doi_enrichment_falls_back_to_url_when_value_is_empty():
    store = EntityStore()
    store.by_type["Dataset"] = [
        Dataset(
            id="Dataset_a.jsonld",
            identifier=PropertyValue(
                propertyID="DOI", url="https://doi.org/10.1234/abc.def"
            ),
        )
    ]

    EnrichmentApplier(store).apply(doi_rule())

    [dataset] = store.of_type("Dataset")
    assert isinstance(dataset.identifier, DOI)
    assert dataset.identifier.value == "10.1234/abc.def"


def test_doi_accepts_a_full_url_as_value():
    assert DOI(value="https://doi.org/10.1234/abc.def").value == "10.1234/abc.def"


def test_doi_without_value_or_usable_url_keeps_original(caplog):
    original = PropertyValue(propertyID="DOI")
    store = EntityStore()
    store.by_type["Dataset"] = [Dataset(id="Dataset_a.jsonld", identifier=original)]

    EnrichmentApplier(store).apply(doi_rule())

    assert store.of_type("Dataset")[0].identifier is original
    assert "Invalid DOI: None" in caplog.text


@pytest.mark.parametrize(
    "cls, kwargs",
    [
        pytest.param(DOI, {}, id="DOI"),
        pytest.param(Orcid, {}, id="Orcid"),
        pytest.param(ISSN, {}, id="ISSN"),
        pytest.param(ISBN, {"name": "x"}, id="ISBN"),
    ],
)
def test_identifier_classes_require_a_value(cls, kwargs):
    with pytest.raises(ValueError, match="Invalid"):
        cls(**kwargs)


def test_identifier_classes_overwrite_submitted_labels():
    doi = DOI(
        value="10.1234/abc.def", propertyID="https://other.org", name="mine"
    )
    assert str(doi.propertyID) == "https://registry.identifiers.org/registry/doi"
    assert doi.name == "Digital Object Identifier"
    orcid = Orcid(value=VALID_ORCID_A, propertyID="ORCID")
    assert str(orcid.propertyID) == "https://registry.identifiers.org/registry/orcid"
