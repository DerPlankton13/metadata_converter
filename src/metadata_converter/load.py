import warnings
from pathlib import Path

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.io import write_json


def load_to_jsonld(schema: SchemaOrgBase, output_dir: Path) -> None:
    """Serialise a schema.org model to standardised JSON-LD and write it to `output_dir`."""
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    with warnings.catch_warnings():
        # Pydantic-core's polymorphic-serialization branch-probing produces spurious
        # "Pydantic serializer warnings" when a submodel narrows an inherited field to
        # exactly `AnyUrl` (e.g. `Orcid.propertyID`) while an ancestor field holding
        # that submodel also has `AnyUrl` as a sibling union branch (e.g.
        # `Thing.identifier`). The dumped value is still correct in every case checked
        # (see tests/schema/test_generated_discrimination.py); this is an upstream
        # pydantic-core quirk, not a data-loss bug. Revisit once pydantic ships a fix
        # (the 2.13 changelog itself notes polymorphic_serialization only addresses
        # serialize_as_any's known issues "in most cases").
        warnings.filterwarnings(
            "ignore", message="Pydantic serializer warnings:", category=UserWarning
        )
        jsonld = schema.model_dump(by_alias=True, exclude_none=True)
    write_json(jsonld, output_dir / generate_filename(jsonld))


def generate_filename(jsonld: dict) -> str:
    """Derive a `.jsonld` filename from the last path component of `@id`."""
    file_name = jsonld["@id"].split("/")[-1]
    if not file_name.endswith(".jsonld"):
        file_name += ".jsonld"
    return file_name
