import pytest

from metadata_converter.biosamples.uplifting import (
    ActionBuilder,
    SampleRecord,
    build_defined_term,
    build_property,
)
from tests.biosamples.conftest import assert_no_diff, make_coord_property, make_property, make_record

EXPECTED_ENVO = {
    "@type": "DefinedTerm",
    "name": "organism name",
    "termCode": "ENVO:12345",
    "url": "https://purl.obolibrary.org/obo/ENVO_12345",
    "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
}

EXPECTED_NCBI = {
    "@type": "DefinedTerm",
    "name": "marine metagenome",
    "termCode": "12345",
    "url": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=12345",
    "inDefinedTermSet": "https://www.ncbi.nlm.nih.gov/Taxonomy",
}


@pytest.mark.parametrize(
    ("input", "expected"),
    [
        pytest.param(
            "organism name [ENVO:12345]", EXPECTED_ENVO, id="envo_square_brackets"
        ),
        pytest.param(
            "organism name (ENVO:12345)", EXPECTED_ENVO, id="envo_round_brackets"
        ),
        pytest.param(
            "I am a (random) description of weired properties [1234]",
            None,
            id="multiple_brackets_returns_none",
        ),
        pytest.param(
            "marine metagenome [NCBI:txid12345]",
            EXPECTED_NCBI,
            id="ncbi_square_brackets",
        ),
        pytest.param(
            "marine metagenome (NCBI:txid12345)",
            EXPECTED_NCBI,
            id="ncbi_round_brackets",
        ),
        pytest.param(
            "nice property [prop:1234]", None, id="unknown_terminology_returns_none"
        ),
    ],
)
def test_build_defined_term(input, expected):
    defined_term = build_defined_term(input)
    assert_no_diff(expected, defined_term)


def test_build_property_multi_value():
    record = make_record(
        make_property(
            name="broad-scale environmental context",
            value="terrestrial biome [ENVO:00000446]|forest biome [ENVO:01000174]|coastal scrubland [ENVO:01000237]",
            value_reference=[
                {
                    "@id": "http://purl.obolibrary.org/obo/ENVO_00000446",
                    "@type": "DefinedTerm",
                },
                {
                    "@id": "http://purl.obolibrary.org/obo/ENVO_01000174",
                    "@type": "DefinedTerm",
                },
                {
                    "@id": "http://purl.obolibrary.org/obo/ENVO_01000237",
                    "@type": "DefinedTerm",
                },
            ],
        )
    )
    expected = {
        "@type": "PropertyValue",
        "name": "broad-scale environmental context",
        "value": [
            "terrestrial biome [ENVO:00000446]",
            "forest biome [ENVO:01000174]",
            "coastal scrubland [ENVO:01000237]",
        ],
        "valueReference": [
            {
                "@type": "DefinedTerm",
                "name": "terrestrial biome",
                "termCode": "ENVO:00000446",
                "url": "https://purl.obolibrary.org/obo/ENVO_00000446",
                "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
            },
            {
                "@type": "DefinedTerm",
                "name": "forest biome",
                "termCode": "ENVO:01000174",
                "url": "https://purl.obolibrary.org/obo/ENVO_01000174",
                "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
            },
            {
                "@type": "DefinedTerm",
                "name": "coastal scrubland",
                "termCode": "ENVO:01000237",
                "url": "https://purl.obolibrary.org/obo/ENVO_01000237",
                "inDefinedTermSet": "https://purl.obolibrary.org/obo/envo.owl",
            },
        ],
    }
    result = build_property(record, "broad-scale environmental context")
    assert_no_diff(expected, result)


def test_build_property_fallback_fixes_https():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={
                "@type": "DefinedTerm",
                "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
            },
        )
    )
    result = build_property(record, "sample collection device")
    assert result["valueReference"] == {
        "@type": "DefinedTerm",
        "@id": "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
    }


def test_build_property_fallback_removes_empty_value_reference():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={"@type": "DefinedTerm"},
        )
    )
    result = build_property(record, "sample collection device")
    assert "valueReference" not in result


def test_build_property_fallback_removes_value_reference_without_information():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={"@type": "DefinedTerm", "@id": ""},
        )
    )
    result = build_property(record, "sample collection device")
    assert "valueReference" not in result


def test_build_property_raises_for_multi_element_single_value():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference=[
                {"@type": "DefinedTerm", "@id": "http://example.com/1"},
                {"@type": "DefinedTerm", "@id": "http://example.com/2"},
            ],
        )
    )
    with pytest.raises(ValueError):
        build_property(record, "sample collection device")


def test_build_location_both_region_and_country():
    record = make_record(
        make_property("geographic location (region and locality)", "Ligurian Sea"),
        make_property("geographic location (country and/or sea)", "Mediterranean Sea"),
        make_property("sampling design label", "SDL-001"),
    )
    expected = {
        "@type": "Place",
        "name": "Ligurian Sea, Mediterranean Sea",
        "geo": None,
        "additionalProperty": [
            {
                "@type": "PropertyValue",
                "propertyID": "https://w3id.org/mixs/0000010",
                "name": "geographic location (country and/or sea,region)",
                "value": "Mediterranean Sea: , Ligurian Sea",
            },
            {
                "@type": "PropertyValue",
                "name": "sampling design label",
                "description": "Sampling Design Label (SDL) is a unique identifier used to track all samples and data originating from the same sampling location. (https://biocean5d.embl.de/faq.cgi)",
                "propertyID": "sampling design label",
                "value": "SDL-001",
            },
        ],
    }
    result = ActionBuilder(SampleRecord(record)).build_location()
    assert_no_diff(expected, result)


def test_build_location_country_only():
    record = make_record(
        make_property("geographic location (country and/or sea)", "Mediterranean Sea"),
    )
    expected = {
        "@type": "Place",
        "name": "Mediterranean Sea",
        "geo": None,
        "additionalProperty": None,
    }
    result = ActionBuilder(SampleRecord(record)).build_location()
    assert_no_diff(expected, result)


def test_build_location_with_coordinates():
    record = make_record(
        make_coord_property("geographic location (latitude)", "43.5", "DD"),
        make_coord_property("geographic location (longitude)", "7.8", "DD"),
        make_coord_property("elevation", "10", "m"),
    )
    result = ActionBuilder(SampleRecord(record)).build_location()

    assert result["name"] is None
    assert result["geo"] == {
        "@type": "GeoCoordinates",
        "latitude": "43.5 DD",
        "longitude": "7.8 DD",
        "elevation": "10 m",
    }


def test_build_location_empty():
    record = make_record()
    result = ActionBuilder(SampleRecord(record)).build_location()

    assert result["name"] is None
    assert result["geo"] is None
    assert result["additionalProperty"] is None


def test_build_instrument_single_value():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette",
            value_reference={
                "@type": "DefinedTerm",
                "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
            },
        )
    )
    result = ActionBuilder(SampleRecord(record)).build_instrument()
    assert result == [
        {
            "@type": "Product",
            "description": "sample collection device",
            "name": "CTD rosette",
            "category": "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
        }
    ]


def test_build_instrument_multi_value():
    record = make_record(
        make_property(
            name="sample collection device",
            value="CTD rosette|Niskin bottle",
            value_reference=[
                {
                    "@type": "DefinedTerm",
                    "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
                },
                {
                    "@type": "DefinedTerm",
                    "@id": "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0412/",
                },
            ],
        )
    )
    result = ActionBuilder(SampleRecord(record)).build_instrument()
    assert result == [
        {
            "@type": "Product",
            "description": "sample collection device",
            "name": ["CTD rosette", "Niskin bottle"],
            "category": [
                "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0017/",
                "https://vocab.nerc.ac.uk/collection/L22/current/TOOL0412/",
            ],
        }
    ]
