from pathlib import Path

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.io import write_json


def load_to_jsonld(schema: SchemaOrgBase, output_dir: Path) -> None:
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    jsonld_dict = schema.model_dump(by_alias=True, exclude_none=True)
    jsonld_dict = {"@context": {"@vocab": "https://schema.org/"}, **jsonld_dict}

    file_name = jsonld_dict["@id"].split("/")[-1]
    if not file_name.endswith(".jsonld"):
        file_name += ".jsonld"
    write_json(jsonld_dict, output_dir / file_name)
