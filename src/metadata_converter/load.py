from pathlib import Path

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.io import write_json


def load_to_jsonld(schema: SchemaOrgBase, output_dir: Path) -> None:
    """Serialise a schema.org model to standardised JSON-LD and write it to `output_dir`."""
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    jsonld = schema.model_dump(by_alias=True, exclude_none=True)
    jsonld = standardise_context(jsonld)

    write_json(jsonld, output_dir / generate_filename(jsonld))


def generate_filename(jsonld: dict) -> str:
    """Derive a `.jsonld` filename from the last path component of `@id`."""
    file_name = jsonld["@id"].split("/")[-1]
    if not file_name.endswith(".jsonld"):
        file_name += ".jsonld"
    return file_name
