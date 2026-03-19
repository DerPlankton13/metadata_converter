import json
from pathlib import Path

from metadata_converter.schema_org_registry import SchemaOrgBase


def load_to_jsonld(schema: SchemaOrgBase, output_path: Path) -> None:
    if type(output_path) is str:
        output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    schema_dict = schema.model_dump()
    jsonld_dict = {
        "@context": {"@vocab": "https://schema.org"},
        "@type": schema_dict.pop("type"),
        "@id": schema_dict.pop("id"),
    }
    for key, value in schema_dict.items():
        if value != [] and value != {} and pd.notna(value):
            jsonld_dict[key] = value

    jsonld_str = json.dumps(jsonld_dict)

    output_path = output_path / (jsonld_dict["@id"] + ".jsonld")
    output_path.write_text(jsonld_str)
