from metadata_converter.utils.jsonld import compact


def test_compact_top_level_type_stripped():
    jsonld = {"@type": "https://schema.org/CreativeWork", "name": "x"}

    result = compact(jsonld, "https://schema.org/")

    assert result["@type"] == "CreativeWork"
    assert result["name"] == "x"


def test_compact_nested_dict_value_stripped():
    jsonld = {"author": {"@type": "https://schema.org/Person"}}

    result = compact(jsonld, "https://schema.org/")

    assert result["author"]["@type"] == "Person"


def test_compact_list_items_stripped():
    jsonld = {
        "creator": [
            {"@type": "https://schema.org/Person"},
            {"@type": "https://schema.org/Organization"},
        ]
    }

    result = compact(jsonld, "https://schema.org/")

    assert result["creator"][0]["@type"] == "Person"
    assert result["creator"][1]["@type"] == "Organization"


def test_compact_context_key_left_untouched():
    jsonld = {
        "@context": "https://schema.org/",
        "@type": "https://schema.org/CreativeWork",
    }

    result = compact(jsonld, "https://schema.org/")

    assert result["@context"] == "https://schema.org/"
    assert result["@type"] == "CreativeWork"


def test_compact_value_nested_under_context_left_untouched():
    jsonld = {"@context": {"@vocab": "https://schema.org/"}}

    result = compact(jsonld, "https://schema.org/")

    assert result["@context"]["@vocab"] == "https://schema.org/"


def test_compact_list_shaped_context_left_untouched():
    jsonld = {
        "@context": [
            "https://schema.org/",
            {"biosample": "https://schema.org/biosample/"},
        ],
        "@type": "https://schema.org/CreativeWork",
    }

    result = compact(jsonld, "https://schema.org/")

    assert result["@context"] == [
        "https://schema.org/",
        {"biosample": "https://schema.org/biosample/"},
    ]
    assert result["@type"] == "CreativeWork"


def test_compact_non_matching_string_left_untouched():
    jsonld = {"identifier": "https://doi.org/10.1234/x"}

    result = compact(jsonld, "https://schema.org/")

    assert result["identifier"] == "https://doi.org/10.1234/x"