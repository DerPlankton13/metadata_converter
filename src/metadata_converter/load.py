import json
from pathlib import Path

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase


def load_to_jsonld(schema: SchemaOrgBase, output_path: Path) -> None:
    if type(output_path) is str:
        output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    jsonld_dict = schema.model_dump(by_alias=True, exclude_none=True)
    jsonld_str = json.dumps(jsonld_dict, indent=2)

    output_path = output_path / (jsonld_dict["@id"] + ".jsonld")
    output_path.write_text(jsonld_str)
