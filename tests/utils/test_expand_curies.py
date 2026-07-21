from metadata_converter.utils.jsonld import expand_curies_in_keys_and_values


def test_expand_curies_in_keys_and_values_empty_prefixes_returns_unchanged():
    jsonld = {"identifier": "biosample:SAMEA111477556"}

    result = expand_curies_in_keys_and_values(jsonld, {})

    assert result == {"identifier": "biosample:SAMEA111477556"}


def test_expand_curies_in_keys_and_values_declared_prefix_in_value_expands():
    jsonld = {"identifier": "biosample:SAMEA111477556"}

    result = expand_curies_in_keys_and_values(
        jsonld, {"biosample": "http://identifiers.org/biosample/"}
    )

    assert result["identifier"] == "http://identifiers.org/biosample/SAMEA111477556"
    # assert that input is left unchanged
    assert jsonld["identifier"] == "biosample:SAMEA111477556"


def test_expand_curies_in_keys_and_values_declared_prefix_in_key_expands():
    jsonld = {"dct:title": "LMO16S sample"}

    result = expand_curies_in_keys_and_values(
        jsonld, {"dct": "http://purl.org/dc/terms/"}
    )

    assert result == {"http://purl.org/dc/terms/title": "LMO16S sample"}


def test_expand_curies_in_keys_and_values_undeclared_prefix_left_unchanged():
    jsonld = {"sameAs": "https://example.org/x"}

    result = expand_curies_in_keys_and_values(
        jsonld, {"biosample": "http://identifiers.org/biosample/"}
    )

    assert result["sameAs"] == "https://example.org/x"


def test_expand_curies_in_keys_and_values_nested_reaches_depth():
    jsonld = {"a": {"b": [{"identifier": "biosample:SAMEA1", "name": "unrelated"}]}}

    result = expand_curies_in_keys_and_values(
        jsonld, {"biosample": "http://identifiers.org/biosample/"}
    )

    nested = result["a"]["b"][0]
    assert nested["identifier"] == "http://identifiers.org/biosample/SAMEA1"
    assert nested["name"] == "unrelated"


def test_expand_curies_in_keys_and_values_prefix_iri_ending_in_underscore():
    jsonld = {"additionalType": "OBI:0000747"}

    result = expand_curies_in_keys_and_values(
        jsonld, {"OBI": "https://purl.obolibrary.org/obo/OBI_"}
    )

    assert result["additionalType"] == "https://purl.obolibrary.org/obo/OBI_0000747"


def test_expand_curies_in_keys_and_values_multiple_prefixes_resolve_independently():
    jsonld = {"dct:title": "LMO16S sample", "identifier": "biosample:SAMEA111477556"}

    result = expand_curies_in_keys_and_values(
        jsonld,
        {
            "biosample": "http://identifiers.org/biosample/",
            "dct": "http://purl.org/dc/terms/",
        },
    )

    assert result["http://purl.org/dc/terms/title"] == "LMO16S sample"
    assert result["identifier"] == "http://identifiers.org/biosample/SAMEA111477556"
