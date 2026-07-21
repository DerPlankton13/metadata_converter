import pytest

from metadata_converter.utils.jsonld import inline_context_prefixes


def test_inline_context_prefixes_missing_sets_default_vocab():
    jsonld = {}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


def test_inline_context_prefixes_already_canonical_is_noop():
    jsonld = {"@context": {"@vocab": "https://schema.org/"}}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


@pytest.mark.parametrize(
    "context_value",
    [
        pytest.param("schema.org", id="bare-host"),
        pytest.param("http://schema.org", id="http-no-slash"),
        pytest.param("https://schema.org/", id="https-with-slash"),
        pytest.param("HTTPS://SCHEMA.ORG", id="uppercase"),
    ],
)
def test_inline_context_prefixes_schema_org_string_variants_normalised(context_value):
    jsonld = {"@context": context_value}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


def test_inline_context_prefixes_schema_org_per_term_string_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": "http://schema.org/name"}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == "http://schema.org/name"
    assert "http://schema.org/name" in caplog.text


def test_inline_context_prefixes_unrelated_string_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": "https://example.org/context.jsonld"}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == "https://example.org/context.jsonld"
    assert "https://example.org/context.jsonld" in caplog.text


@pytest.mark.parametrize(
    "context_value",
    [
        pytest.param(
            [
                "http://schema.org",
                {
                    "OBI": "http://purl.obolibrary.org/obo/OBI_",
                    "biosample": "http://identifiers.org/biosample/",
                },
            ],
            id="single-dict-two-prefixes",
        ),
        pytest.param(
            ["http://schema.org", {"foo": "bar"}, {"baz": "qux"}],
            id="multiple-dicts",
        ),
    ],
)
def test_inline_context_prefixes_list_context_collapses_to_bare_vocab(context_value):
    jsonld = {"@context": context_value}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == {"@vocab": "https://schema.org/"}


def test_inline_context_prefixes_list_context_expands_value_using_derived_prefix():
    jsonld = {
        "@context": ["http://schema.org", {"biosample": "http://identifiers.org/biosample/"}],
        "identifier": "biosample:SAMEA111477556",
    }

    result = inline_context_prefixes(jsonld)

    assert result["identifier"] == "http://identifiers.org/biosample/SAMEA111477556"
    assert result["@context"] == {"@vocab": "https://schema.org/"}


def test_inline_context_prefixes_list_context_expands_key_using_derived_prefix():
    jsonld = {
        "@context": ["http://schema.org", {"dct": "http://purl.org/dc/terms/"}],
        "dct:title": "LMO16S sample",
    }

    result = inline_context_prefixes(jsonld)

    assert result == {
        "@context": {"@vocab": "https://schema.org/"},
        "http://purl.org/dc/terms/title": "LMO16S sample",
    }


def test_inline_context_prefixes_list_context_undeclared_prefix_value_left_unchanged():
    jsonld = {
        "@context": ["http://schema.org", {"biosample": "http://identifiers.org/biosample/"}],
        "additionalType": "OBI:0000747",
    }

    result = inline_context_prefixes(jsonld)

    assert result["additionalType"] == "OBI:0000747"


def test_inline_context_prefixes_list_wrong_order_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": [{"foo": "bar"}, "http://schema.org"]}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == [{"foo": "bar"}, "http://schema.org"]
    assert "{'foo': 'bar'}" in caplog.text


def test_inline_context_prefixes_list_second_element_not_dict_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": ["http://schema.org", "http://example.org"]}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == ["http://schema.org", "http://example.org"]
    assert "http://example.org" in caplog.text


def test_inline_context_prefixes_list_first_element_not_schema_org_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": ["http://example.org", {"foo": "bar"}]}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == ["http://example.org", {"foo": "bar"}]
    assert "http://example.org" in caplog.text


def test_inline_context_prefixes_unrelated_dict_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": {"foo": "bar"}}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == {"foo": "bar"}
    assert "{'foo': 'bar'}" in caplog.text


def test_inline_context_prefixes_dict_with_schema_org_key_logs_and_leaves_untouched(caplog):
    jsonld = {"@context": {"schema": "http://schema.org"}}

    result = inline_context_prefixes(jsonld)

    assert result["@context"] == {"schema": "http://schema.org"}
    assert "http://schema.org" in caplog.text
