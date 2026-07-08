import re
from pathlib import Path
from typing import Any

from metadata_converter.schema_org_models.schemaorg_models import SchemaOrgBase
from metadata_converter.utils.hashing import get_type, hashed_id
from metadata_converter.utils.io import write_json


def load_to_jsonld(schema: SchemaOrgBase, output_dir: Path, keep_id: bool = False) -> None:
    """Serialise a schema.org model to standardised JSON-LD and write it to `output_dir`.

    Parameters
    ----------
    schema
        The schema.org model to serialise.
    output_dir
        Directory to write the resulting `.jsonld` file into.
    keep_id
        If `False` (default), `standardise_id` replaces the model's `@id` with a
        content hash unless it already follows that convention. Pass `True` to
        write the `@id` as-is instead — for entities whose `@id` was already
        standardised at an earlier stage (e.g. re-exporting an entity after
        uplift refines it in place), so it isn't rehashed based on content that
        has since changed, which would orphan cross-references other entities
        already resolved against the original id.
    """
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    jsonld = schema.model_dump(by_alias=True, exclude_none=True)
    if not keep_id:
        jsonld = standardise_id(jsonld)
    jsonld = {"@context": {"@vocab": "https://schema.org/"}, **jsonld}

    write_json(jsonld, output_dir / generate_filename(jsonld))


def generate_filename(jsonld: dict) -> str:
    """Derive a `.jsonld` filename from the last path component of `@id`."""
    file_name = jsonld["@id"].split("/")[-1]
    if not file_name.endswith(".jsonld"):
        file_name += ".jsonld"
    return file_name


def standardise_id(jsonld: dict) -> dict:
    """Replace a non-hashed `@id` with a content hash, preserving the original as `identifier`.

    Mutates and returns `jsonld` in place.
    """
    current_id = jsonld.get("@id")
    if not is_hashed_id(current_id, get_type(jsonld)):
        jsonld["@id"] = hashed_id({k: v for k, v in jsonld.items() if k != "@id"})
        if current_id is not None and not (
            in_property(jsonld.get("identifier"), current_id)
            or in_property(jsonld.get("url"), current_id)
        ):
            jsonld["identifier"] = current_id
    return jsonld


def is_hashed_id(current_id: str, schema_type: str) -> bool:
    """Check whether `current_id` already follows the `<schema_type>_<hash>.jsonld` convention."""
    if current_id is None:
        return False
    file_name = current_id.split("/")[-1].removesuffix(".jsonld")
    pattern = rf"{re.escape(schema_type)}_[A-Za-z0-9_-]{{22}}"
    return re.fullmatch(pattern, file_name) is not None


def in_property(prop: Any, value: Any) -> bool:
    """Check whether `value` equals `prop` or is contained in it when `prop` is a list."""
    if prop is None:
        return False
    if not isinstance(prop, list):
        prop = [prop]
    if value in prop:
        return True
    return False


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
    already in canonical vocab form — raises, since there is no safe way to infer
    intent from it.
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
    raise ValueError(f"Unsupported @context value: {current_context!r}")
