"""Tests for ``EnrichmentApplier`` in ``uplift/enrichment.py`` and the overlap validator.

Enrichment wraps a scalar property value in a project-defined PropertyValue
subclass (Orcid, DOI, …). The class's Pydantic validators auto-populate the
metadata fields (url, name, propertyID, etc.).
"""

import pytest

from metadata_converter.flat_data.uplift.config import EnrichmentRule, FlatDataUpliftConfig, LinkRule
from metadata_converter.flat_data.uplift.enrichment import EnrichmentApplier
from metadata_converter.flat_data.uplift.entity_store import EntityStore
from metadata_converter.schema_org_models.custom_models import Orcid
from metadata_converter.schema_org_models.schemaorg_models import Person

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
# Overlap validator on FlatDataUpliftConfig
# ---------------------------------------------------------------------------


def test_two_enrichments_on_same_target_raise_at_config_load(tmp_path):
    with pytest.raises(
        ValueError, match=r"Person\.identifier.*targeted by multiple uplift rules"
    ):
        FlatDataUpliftConfig(
            input_dir=tmp_path / "in",
            output_dir=tmp_path / "out",
            enrichments=[
                EnrichmentRule(
                    on_type="Person", target_property="identifier", enrich_as="Orcid"
                ),
                EnrichmentRule(
                    on_type="Person", target_property="identifier", enrich_as="DOI"
                ),
            ],
        )


def test_link_and_enrichment_on_same_target_raise_at_config_load(tmp_path):
    with pytest.raises(
        ValueError, match=r"Person\.identifier.*'link'.*'enrichment'"
    ):
        FlatDataUpliftConfig(
            input_dir=tmp_path / "in",
            output_dir=tmp_path / "out",
            links=[
                LinkRule(
                    on_type="Person",
                    target_property="identifier",
                    match_literal="x",
                    in_type="Orcid",
                    in_property="value",
                ),
            ],
            enrichments=[
                EnrichmentRule(
                    on_type="Person", target_property="identifier", enrich_as="Orcid"
                ),
            ],
        )
