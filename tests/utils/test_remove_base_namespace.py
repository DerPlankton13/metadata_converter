from metadata_converter.utils.jsonld import remove_base_namespace


def test_remove_base_namespace_top_level_type_stripped():
    jsonld = {"@type": "https://schema.org/CreativeWork", "name": "x"}

    result = remove_base_namespace(jsonld, "https://schema.org/")

    assert result["@type"] == "CreativeWork"
    assert result["name"] == "x"


def test_remove_base_namespace_nested_dict_value_stripped():
    jsonld = {"author": {"@type": "https://schema.org/Person"}}

    result = remove_base_namespace(jsonld, "https://schema.org/")

    assert result["author"]["@type"] == "Person"


def test_remove_base_namespace_list_items_stripped():
    jsonld = {
        "creator": [
            {"@type": "https://schema.org/Person"},
            {"@type": "https://schema.org/Organization"},
        ]
    }

    result = remove_base_namespace(jsonld, "https://schema.org/")

    assert result["creator"][0]["@type"] == "Person"
    assert result["creator"][1]["@type"] == "Organization"


def test_remove_base_namespace_context_key_left_untouched():
    jsonld = {
        "@context": "https://schema.org/",
        "@type": "https://schema.org/CreativeWork",
    }

    result = remove_base_namespace(jsonld, "https://schema.org/")

    assert result["@context"] == "https://schema.org/"
    assert result["@type"] == "CreativeWork"


def test_remove_base_namespace_value_nested_under_context_left_untouched():
    jsonld = {"@context": {"@vocab": "https://schema.org/"}}

    result = remove_base_namespace(jsonld, "https://schema.org/")

    assert result["@context"]["@vocab"] == "https://schema.org/"


def test_remove_base_namespace_list_shaped_context_left_untouched():
    jsonld = {
        "@context": [
            "https://schema.org/",
            {"biosample": "https://schema.org/biosample/"},
        ],
        "@type": "https://schema.org/CreativeWork",
    }

    result = remove_base_namespace(jsonld, "https://schema.org/")

    assert result["@context"] == [
        "https://schema.org/",
        {"biosample": "https://schema.org/biosample/"},
    ]
    assert result["@type"] == "CreativeWork"


def test_remove_base_namespace_non_matching_string_left_untouched():
    jsonld = {"identifier": "https://doi.org/10.1234/x"}

    result = remove_base_namespace(jsonld, "https://schema.org/")

    assert result["identifier"] == "https://doi.org/10.1234/x"
