import logging
from pathlib import Path

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.io import write_json

logger = logging.getLogger(__name__)


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


def is_schema_org_root(value: str) -> bool:
    """Check whether `value` refers to schema.org's root (not a per-term IRI)."""
    lowered = value.lower()
    if "schema.org" not in lowered:
        return False
    after = lowered.split("schema.org", 1)[1]
    return after in ("", "/")


def standardise_context(jsonld: dict) -> dict:
    """Normalise `@context` to `{"@vocab": "https://schema.org/", ...}`.

    Accepts a missing context, a bare schema.org string (in any host/scheme/case
    variant), or a list of a bare schema.org string followed by one or more dicts
    of extra prefix mappings (the shape BioSamples emits). Any other shape —
    including a per-term schema.org IRI, an unrelated string, or a dict not
    already in canonical vocab form — is left untouched and logged as an error,
    since there is no safe way to infer intent from it.
    """
    current_context = jsonld.get("@context")
    if current_context is None or current_context == {"@vocab": "https://schema.org/"}:
        jsonld["@context"] = {"@vocab": "https://schema.org/"}
        return jsonld
    if isinstance(current_context, str) and is_schema_org_root(current_context):
        jsonld["@context"] = {"@vocab": "https://schema.org/"}
        return jsonld
    if (
        isinstance(current_context, list)
        and len(current_context) >= 2
        and isinstance(current_context[0], str)
        and is_schema_org_root(current_context[0])
        and all(isinstance(entry, dict) for entry in current_context[1:])
    ):
        merged = {"@vocab": "https://schema.org/"}
        for entry in current_context[1:]:
            merged.update(entry)
        jsonld["@context"] = merged
        return jsonld
    logger.error("Unsupported @context value: %r", current_context)
    return jsonld
