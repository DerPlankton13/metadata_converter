import json

import pytest

from metadata_converter.load import load_to_jsonld
from metadata_converter.schema_org_models.schemaorg_models import Person


def test_load_to_jsonld_writes_file_with_id_verbatim(tmp_path):
    schema = Person(
        id="Person_orig-id.jsonld",
        name="Ada Lovelace",
        context={"@vocab": "https://schema.org/"},
    )

    load_to_jsonld(schema, tmp_path)

    written = tmp_path / "Person_orig-id.jsonld"
    assert written.exists()
    content = json.loads(written.read_text())
    assert content["@context"] == {"@vocab": "https://schema.org/"}
    assert content["@id"] == "Person_orig-id.jsonld"
    assert content["name"] == "Ada Lovelace"


def test_load_to_jsonld_string_output_dir_writes_file(tmp_path):
    schema = Person(
        id="Person_orig-id.jsonld",
        name="Ada Lovelace",
        context={"@vocab": "https://schema.org/"},
    )

    load_to_jsonld(schema, str(tmp_path))

    assert (tmp_path / "Person_orig-id.jsonld").exists()


def test_load_to_jsonld_no_context_raises(tmp_path):
    schema = Person(id="Person_orig-id.jsonld", name="Ada Lovelace")

    with pytest.raises(ValueError, match="Person_orig-id.jsonld.*no @context"):
        load_to_jsonld(schema, tmp_path)
