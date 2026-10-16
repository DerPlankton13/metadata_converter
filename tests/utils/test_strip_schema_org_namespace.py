from metadata_converter.utils.jsonld import strip_schema_org_namespace


def test_strip_schema_org_namespace_top_level_type_stripped():
    jsonld = {"@type": "https://schema.org/CreativeWork", "name": "x"}

    result = strip_schema_org_namespace(jsonld)

    assert result["@type"] == "CreativeWork"
    assert result["name"] == "x"


def test_strip_schema_org_namespace_nested_dict_value_stripped():
    jsonld = {"author": {"@type": "https://schema.org/Person"}}

    result = strip_schema_org_namespace(jsonld)

    assert result["author"]["@type"] == "Person"


def test_strip_schema_org_namespace_list_items_stripped():
    jsonld = {
        "creator": [
            {"@type": "https://schema.org/Person"},
            {"@type": "https://schema.org/Organization"},
        ]
    }

    result = strip_schema_org_namespace(jsonld)

    assert result["creator"][0]["@type"] == "Person"
    assert result["creator"][1]["@type"] == "Organization"


def test_strip_schema_org_namespace_context_key_left_untouched():
    jsonld = {
        "@context": "https://schema.org/",
        "@type": "https://schema.org/CreativeWork",
    }

    result = strip_schema_org_namespace(jsonld)

    assert result["@context"] == "https://schema.org/"
    assert result["@type"] == "CreativeWork"


def test_strip_schema_org_namespace_value_nested_under_context_left_untouched():
    jsonld = {"@context": {"@vocab": "https://schema.org/"}}

    result = strip_schema_org_namespace(jsonld)

    assert result["@context"]["@vocab"] == "https://schema.org/"


def test_strip_schema_org_namespace_list_shaped_context_left_untouched():
    jsonld = {
        "@context": [
            "https://schema.org/",
            {"biosample": "https://schema.org/biosample/"},
        ],
        "@type": "https://schema.org/CreativeWork",
    }

    result = strip_schema_org_namespace(jsonld)

    assert result["@context"] == [
        "https://schema.org/",
        {"biosample": "https://schema.org/biosample/"},
    ]
    assert result["@type"] == "CreativeWork"


def test_strip_schema_org_namespace_non_matching_string_left_untouched():
    jsonld = {"identifier": "https://doi.org/10.1234/x"}

    result = strip_schema_org_namespace(jsonld)

    assert result["identifier"] == "https://doi.org/10.1234/x"
