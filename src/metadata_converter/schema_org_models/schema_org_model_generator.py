"""
Generate Pydantic v2 models from schema.org JSON-LD.

Run once to produce a static `schemaorg_models.py` file with no runtime cost.

Usage
-----
Generate all schema.org types::

    python schemaorg_codegen.py

Enforce exact field types (no str fallback for typed fields)::

    python schemaorg_codegen.py --strict

Write to a custom path::

    python schemaorg_codegen.py --out my_models.py

Then in your application::

    from schemaorg_models import Person

    person = Person(name="Ada Lovelace", email="ada@example.com")
"""

import argparse
import inspect
import json
import re
import subprocess
import sys
import textwrap
import urllib.request
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from keyword import iskeyword
from pathlib import Path
from typing import Any, TypedDict

from pydantic import AnyUrl, BaseModel, ConfigDict, Field
from pydantic.fields import FieldInfo

SCHEMA_URL = "https://schema.org/version/latest/schemaorg-current-https.jsonld"
SCHEMA_PREFIX = "https://schema.org/"
DEFAULT_OUTPUT_PATH = Path(__file__).parent / "schemaorg_models.py"

# Maps primitive schema.org type names to Python types.
PRIMITIVE_TYPE_MAP: dict[str, Any] = {
    "Text": str,
    "URL": AnyUrl,
    "Boolean": bool,
    "Number": float,
    "Integer": int,
    "Float": float,
    "Date": date,
    "DateTime": datetime,
    "Time": time,
    "Duration": timedelta,
    "XPathType": str,
    "CssSelectorType": str,
    "PronounceableText": str,
}

# Maps Python type objects to their source-code names for the generated module.
PRIMITIVE_SOURCE: dict[Any, str] = {
    str: "str",
    bool: "bool",
    float: "float",
    int: "int",
    date: "date",
    datetime: "datetime",
    time: "time",
    timedelta: "timedelta",
    AnyUrl: "AnyUrl",
}


class ClassDef(TypedDict):
    """Parsed metadata for a single schema.org class."""

    parents: list[str]
    schema_name: str
    comment: str


class FieldDef(TypedDict):
    """Parsed metadata for a single schema.org property."""

    name: str
    schema_name: str
    allowed_types: list[str]
    comment: str


class SchemaOrgBase(BaseModel):
    """
    Base class for all schema.org Pydantic models.

    Provides the two fields common to all JSON-LD nodes and configures
    Pydantic to accept both Python attribute names and JSON-LD @-prefixed
    aliases interchangeably.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True, defer_build=True)

    # The schema.org class name, will be set automatically by each generated subclass.
    type: str = Field(alias="@type")
    id: str | None = Field(default=None, alias="@id")


def get_schema(type_name: str) -> type[SchemaOrgBase]:
    """
    Return the Pydantic model class for a schema.org type name.

    Parameters
    ----------
    type_name : str
        Schema.org class name (e.g. "Person").

    Returns
    -------
    type[SchemaOrgBase]

    Raises
    ------
    KeyError
        If the type_name is not available.

    Examples
    --------
    ::

        cls = get_schema("Person")
        instance = cls(**data)
    """
    cls = globals().get(type_name)
    if cls is None or not (isinstance(cls, type) and issubclass(cls, SchemaOrgBase)):
        raise KeyError(
            f"{type_name!r} is not a known schema.org type. Ensure that it is available in schema.org and update the Pydantic models if necessary."
        )
    return cls


def _local(iri: str) -> str:
    """Extract local name from a schema.org IRI."""
    return iri.removeprefix(SCHEMA_PREFIX).removeprefix("schema:")


def _safe_name(name: str) -> str:
    """
    Return a valid Python identifier for a schema.org local name.

    Applies the minimum change needed to make the name usable as a Python
    class name or attribute:

    - Names starting with a digit get a leading underscore (e.g.
      ``"3DModel"`` → ``"_3DModel"``).
    - Python reserved keywords get a trailing underscore (e.g.
      ``"yield"`` → ``"yield_"``, following PEP 8 convention).
    - Names that are already valid are returned unchanged.

    When the safe name differs from the original, it needs to be stored
    as a Pydantic ``alias`` so serialisation uses the correct
    schema.org name.

    Parameters
    ----------
    name : str
        A schema.org local name.

    Returns
    -------
    str
        A valid Python identifier.
    """
    if not name[0].isalpha() and name[0] != "_":
        name = "_" + name
    if iskeyword(name):
        name = name + "_"
    return name


def _clean_comment(comment: str) -> str:
    """
    Sanitise a raw schema.org rdfs:comment for use as a Python docstring.

    Schema.org comments contain MediaWiki markup that is not valid in Python
    source. This function removes or replaces the patterns that would cause
    ``SyntaxWarning`` or ``SyntaxError`` in the generated file, and wraps
    long lines to keep the generated source readable.

    Parameters
    ----------
    comment : str
        The raw comment string from the JSON-LD ``rdfs:comment`` field.

    Returns
    -------
    str
        A clean string safe to embed in a Python docstring, with lines
        wrapped to at most 84 characters (leaving room for 4-space indentation
        to stay within the conventional 88-character line limit).
    """
    # [[ClassName]] -> ClassName  (MediaWiki internal links)
    comment = re.sub(r"\[\[([^\]]+)\]\]", r"\1", comment)
    # Remove backslashes to avoid invalid escape sequence warnings
    comment = comment.replace("\\", "")
    # Replace triple quotes to avoid breaking the docstring delimiter
    comment = comment.replace('"""', "'''")
    # Wrap each paragraph individually, preserving existing newlines
    paragraphs = comment.split("\n")
    wrapped = [textwrap.fill(p, width=84) if p.strip() else "" for p in paragraphs]
    return "\n".join(wrapped)


def _schema_ids(node: dict, key: str) -> list[str]:
    """
    Extract safe Python names from a JSON-LD node's @id references.

    Reads the value at ``key`` from ``node``, normalises it to a list,
    filters to schema.org IRIs only, and returns the local name of each
    as a safe Python identifier.

    Parameters
    ----------
    node : dict
        A single entry from the JSON-LD ``@graph``.
    key : str
        The predicate to read, e.g. ``"schema:domainIncludes"``.

    Returns
    -------
    list[str]
        Safe Python identifiers for each referenced schema.org type.
    """
    val = node.get(key, [])
    if isinstance(val, dict):
        val = [val]
    return [
        _safe_name(_local(item["@id"]))
        for item in val
        if isinstance(item, dict) and item.get("@id", "").startswith("schema:")
    ]


def parse_schema(data: dict) -> tuple[dict[str, ClassDef], dict[str, list[FieldDef]]]:
    """
    Extract classes and properties from schema.org JSON-LD.

    Parameters
    ----------
    data : dict
        Parsed JSON-LD document.

    Returns
    -------
    tuple[dict[str, ClassDef], dict[str, list[FieldDef]]]
        (classes, class_fields)

    Notes
    -----
    - `classes` contains metadata per class
    - `class_fields` maps class → list of property definitions
    """
    graph = data.get("@graph", [])
    classes: dict[str, ClassDef] = {}
    class_fields: dict[str, list[FieldDef]] = defaultdict(list)

    for node in graph:
        node_id = node.get("@id", "")
        if not node_id.startswith("schema:"):
            continue

        schema_name = _local(node_id)
        py_name = _safe_name(schema_name)

        rdf_types = node.get("@type", [])
        if isinstance(rdf_types, str):
            rdf_types = [rdf_types]

        comment_raw = node.get("rdfs:comment", "")
        if isinstance(comment_raw, dict):
            comment_text = comment_raw.get("@value", "")
        else:
            comment_text = str(comment_raw)
        comment = _clean_comment(comment_text)

        if "rdfs:Class" in rdf_types:
            parents = _schema_ids(node, "rdfs:subClassOf")
            classes[py_name] = {
                "parents": parents,
                "schema_name": schema_name,
                "comment": comment,
            }

        elif "rdf:Property" in rdf_types:
            owner_classes = _schema_ids(node, "schema:domainIncludes")
            allowed_types = _schema_ids(node, "schema:rangeIncludes")

            for owner_class in owner_classes:
                class_fields[owner_class].append(
                    {
                        "name": py_name,
                        "schema_name": schema_name,
                        "allowed_types": allowed_types,
                        "comment": comment,
                    }
                )

    class_fields = {
        class_name: fields
        for class_name, fields in class_fields.items()
        if class_name in classes
    }

    return classes, class_fields


def _resolve_type(allowed_types: list[str], strict: bool) -> str:
    """
    Translate a list of schema.org allowed type names into a type annotation string.

    Parameters
    ----------
    allowed_types : list[str]
        Local schema.org type names, e.g. ``["Text", "URL"]``.
    strict : bool
        When ``False``, ``str`` is appended as a fallback type.

    Returns
    -------
    str
        A type annotation string for the generated module, e.g.
        ``"str | list[str] | None"``. Falls back to ``"Any | None"``
        when ``allowed_types`` is empty.
    """
    if not allowed_types:
        # No rangeIncludes declared in schema.org — type is unknown.
        return "Any | None"

    source_names: list[str] = []
    for type_name in allowed_types:
        if type_name in PRIMITIVE_TYPE_MAP:
            source_names.append(PRIMITIVE_SOURCE[PRIMITIVE_TYPE_MAP[type_name]])
        else:
            source_names.append(type_name)

    if not strict and "str" not in source_names:
        source_names.append("str")

    src = " | ".join(source_names)
    return f"{src} | list[{src}] | None"


def build_models(
    classes: dict[str, ClassDef],
    class_fields: dict[str, list[FieldDef]],
    strict: bool,
) -> dict[str, dict]:
    """
    Build metadata for all models to be rendered.

    Parameters
    ----------
    classes : dict[str, ClassDef]
        Parsed class metadata from ``parse_schema``.
    class_fields : dict[str, list[FieldDef]]
        Inverted property index from ``parse_schema``.
    strict : bool

    Returns
    -------
    dict[str, dict]
        Metadata for each class, keyed by Python-safe name.
    """
    return {
        class_name: {
            "parents": class_def["parents"],
            "schema_name": class_def["schema_name"],
            "comment": class_def["comment"],
            "fields": {
                field["name"]: (
                    _resolve_type(field["allowed_types"], strict),
                    Field(
                        default=None,
                        alias=field["schema_name"]
                        if field["name"] != field["schema_name"]
                        else None,
                    ),
                )
                for field in class_fields.get(class_name, [])
            },
        }
        for class_name, class_def in classes.items()
    }


def _render_field(name: str, type_str: str, field_info: FieldInfo) -> str:
    """Render a single schema.org property field as a source code line."""
    args = ["default=None"]
    if field_info.alias:
        args.append(f'alias="{field_info.alias}"')
    return f"{name}: {type_str} = Field({', '.join(args)})"


def _topological_sort(models: dict[str, dict]) -> list[str]:
    """Return model names sorted so every parent appears before its children."""
    visited: set[str] = set()
    order: list[str] = []

    def visit(class_name: str) -> None:
        if class_name in visited or class_name not in models:
            return
        visited.add(class_name)
        for parent in models[class_name]["parents"]:
            visit(parent)
        order.append(class_name)

    for class_name in sorted(models):
        visit(class_name)
    return order


def render_module(models: dict[str, dict], strict: bool) -> str:
    """
    Render all models into a Python module string.

    Parameters
    ----------
    models : dict[str, dict]
        Model metadata as returned by ``build_models``.
    strict : bool
        Recorded in the module header for documentation purposes.

    Returns
    -------
    str
        The full source of the generated Python module.
    """
    order = _topological_sort(models)

    lines = [
        '"""',
        "Auto-generated schema.org Pydantic models.",
        "Generated by schemaorg_codegen.py -- do not edit manually.",
        "",
        "Settings",
        "--------",
        f"strict : {strict}",
        '"""',
        "from __future__ import annotations",
        "",
        "from datetime import date, datetime, time, timedelta",
        "from typing import Any",
        "",
        "from pydantic import AnyUrl, BaseModel, ConfigDict, Field",
        "",
        "",
        inspect.getsource(SchemaOrgBase),
        "",
    ]

    for class_name in order:
        cls = models[class_name]

        parent = cls["parents"][0] if cls["parents"] else "SchemaOrgBase"
        lines.append(f"class {class_name}({parent}):")

        doc = cls["comment"] or f"schema.org/{class_name}"
        indented_doc = doc.replace("\n", "\n    ")
        lines.append(f'    """{indented_doc}"""')
        lines.append("")

        schema_name = cls["schema_name"]
        lines.append(f'    type: str = Field(default="{schema_name}", alias="@type")')

        for field_name, (type_str, field_info) in cls["fields"].items():
            lines.append(f"    {_render_field(field_name, type_str, field_info)}")

        lines.append("")

    lines.append("")
    lines.append(
        "# ---------------------------------------------------------------------------"
    )
    lines.append("# Dynamic lookup")
    lines.append(
        "# ---------------------------------------------------------------------------"
    )
    lines.append("")
    lines.append(inspect.getsource(get_schema))

    return "\n".join(lines)


def generate(
    strict: bool = False,
    out: Path = DEFAULT_OUTPUT_PATH,
) -> None:
    """
    Run the full generation pipeline and write the output module.

    Parameters
    ----------
    strict : bool, optional
        When ``True``, field types are enforced exactly as declared in
        schema.org. When ``False`` (the default), ``str`` is accepted as
        a fallback for any typed field.
    out : Path, optional
        Destination path for the generated Python module.
    """
    print(f"Downloading schema.org from {SCHEMA_URL} ...")
    with urllib.request.urlopen(SCHEMA_URL) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    print("Parsing schema.org vocabulary ...")
    classes, class_fields = parse_schema(data)
    print(f"  Found {len(classes)} classes")

    print(f"Building {len(classes)} model definitions ...")
    models = build_models(classes, class_fields, strict)

    print("Rendering source code...")
    source = render_module(models, strict)

    out.write_text(source, encoding="utf-8")
    print(f"Written to {out} ({len(models)} models)")

    print("Verifying all classes can be instantiated ...")
    verify_script = Path(__file__).parent / "check_schemaorg_models.py"
    subprocess.run([sys.executable, str(verify_script)], check=True)


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Generate static Pydantic models from schema.org"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Enforce exact schema.org field types (no str fallback).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output file (default: {DEFAULT_OUTPUT_PATH})",
    )
    args = parser.parse_args()
    generate(strict=args.strict, out=args.out)


if __name__ == "__main__":
    main()
