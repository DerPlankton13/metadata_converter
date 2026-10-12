"""Tests for ``EnrichmentApplier`` in ``uplift/enrichment.py`` and the overlap validator.

Enrichment wraps a scalar property value in a project-defined PropertyValue
subclass (Orcid, DOI, …). The class's Pydantic validators auto-populate the
metadata fields (url, name, propertyID, etc.).
"""

import pytest

from metadata_converter.uplift.config import EnrichmentRule, GenericUpliftConfig
from metadata_converter.uplift.enrichment import EnrichmentApplier
from metadata_converter.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.custom_models import DOI, Orcid
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
