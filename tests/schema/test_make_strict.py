"""Tests for ``make_strict`` (schemaorg_models).

``make_strict`` builds a dynamically-created subclass whose forward references
(``PropertyValue``, ``Organization``, …) must be re-resolved. Under Pydantic >= 2.12
that resolution uses the *caller's* namespace, so the subclass only works if
``make_strict`` binds it to the model's own module. These tests therefore import
**only** ``make_strict`` and one forward-ref'd model — not the referenced types —
reproducing the caller context that previously raised "not fully defined".
"""
import pytest
from pydantic import ValidationError

from metadata_converter.schema_org_models.schemaorg_models import Project, make_strict


def test_make_strict_accepts_valid_input():
    strict = make_strict(Project)

    instance = strict.model_validate({"@id": "project.jsonld"})

    assert instance.id == "project.jsonld"


def test_make_strict_rejects_extra_field():
    strict = make_strict(Project)

    with pytest.raises(ValidationError, match="notarealfield"):
        strict.model_validate({"@id": "project.jsonld", "notarealfield": "x"})
