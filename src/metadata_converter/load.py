from pathlib import Path

from metadata_converter.io import write_json
from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase


def load_to_jsonld(schema: SchemaOrgBase, output_path: Path) -> None:
    if isinstance(output_path, str):
        output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    jsonld_dict = schema.model_dump(by_alias=True, exclude_none=True)
    jsonld_dict = {"@context": {"@vocab": "https://schema.org"}, **jsonld_dict}

    file_name = jsonld_dict["@id"].split("/")[-1]
    if not file_name.endswith(".jsonld"):
        file_name += ".jsonld"
    write_json(jsonld_dict, output_path / file_name)
