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
import textwrap
import urllib.request
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from keyword import iskeyword
from pathlib import Path
from typing import Any, TypedDict

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, create_model
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

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # The schema.org class name, will be set automatically by each generated subclass.
    type: str = Field(alias="@type")
    id: Optional[str] = Field(default=None, alias="@id")


def get_schema(type_name: str) -> "type[SchemaOrgBase]":
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

    # Remove class_fields entries for classes not in the schema
    class_fields = {
        class_name: fields
        for class_name, fields in class_fields.items()
        if class_name in classes
    }

    return classes, class_fields


def _resolve_type(
    allowed_types: list[str],
    model_cache: dict[str, type[BaseModel] | None],
    strict: bool,
) -> tuple[Any, str]:
    """
    Translate a list of schema.org allowed type names into a Python type and
    its source-code string representation, returned together as a pair.

    Parameters
    ----------
    allowed_types : list[str]
        Local schema.org type names, e.g. ``["Text", "URL"]``.
    model_cache : dict
        The partially-built model cache. May contain ``None`` placeholders
        for classes currently being built (circular references) — these
        are filtered out.
    strict : bool
        When ``False``, ``str`` is appended as a fallback type.

    Returns Optional[T | list[T]] to allow single or multiple values.
    """
    fallback = (Optional[Any], "Optional[Any]")

    if not allowed_types:
        return fallback

    python_types: list[Any] = []
    source_names: list[str] = []
    for type_name in allowed_types:
        if type_name in PRIMITIVE_TYPE_MAP:
            tp = PRIMITIVE_TYPE_MAP[type_name]
            python_types.append(tp)
            source_names.append(PRIMITIVE_SOURCE[tp])
        else:
            # Another schema.org class. May be None if currently being built
            # due to a circular reference — skip it in that case.
            resolved = model_cache.get(type_name)
            if resolved is not None:
                python_types.append(resolved)
                source_names.append(type_name)

    if not python_types:
        return fallback

    if not strict and str not in python_types:
        python_types.append(str)
        source_names.append("str")

    type_union = python_types[0]
    for t in python_types[1:]:
        type_union |= t

    src = " | ".join(source_names)
    full = f"Optional[{src} | list[{src}]]"

    return Optional[type_union | list[type_union]], full


def _build_fields(
    class_name: str,
    class_fields: dict[str, list[FieldDef]],
    cache: dict[str, type[BaseModel] | None],
    strict: bool,
) -> dict[str, Any]:
    """Build the Pydantic field definitions for a single class."""
    field_defs: dict[str, Any] = {}

    for field in class_fields.get(class_name, []):
        python_types, source = _resolve_type(field["allowed_types"], cache, strict)
        alias = field["schema_name"] if field["name"] != field["schema_name"] else None
        field_defs[field["name"]] = (
            python_types,
            Field(default=None, alias=alias, json_schema_extra={"source": source}),
        )

    return field_defs


def build_models(
    classes: dict[str, ClassDef],
    class_fields: dict[str, list[FieldDef]],
    strict: bool,
) -> dict[str, type[BaseModel]]:
    """
    Build all Pydantic models from parsed schema data.

    Uses a ``None`` placeholder in the cache to handle circular references —
    any class currently being built returns ``None`` when referenced as a
    field type, which ``_resolve_type`` treats as "not yet available" and
    skips.

    Parameters
    ----------
    classes : dict[str, ClassDef]
        Parsed class metadata from ``parse_schema``.
    class_fields : dict[str, list[FieldDef]]
        Inverted property index from ``parse_schema``.
    strict : bool

    Returns
    -------
    dict[str, type[BaseModel]]
        All successfully built Pydantic model classes, keyed by name.
    """
    cache: dict[str, type[BaseModel] | None] = {}

    def build(class_name: str) -> type[BaseModel] | None:
        if class_name in cache:
            return cache[class_name]

        # Placeholder to break circular references
        cache[class_name] = None

        class_def = classes[class_name]

        parents = []
        for parent_name in class_def["parents"]:
            if parent_name in classes:
                parent_model = build(parent_name)
                if parent_model is not None:
                    parents.append(parent_model)
        base = parents[0] if parents else SchemaOrgBase

        field_defs = _build_fields(class_name, class_fields, cache, strict)

        # The type field default is always the original schema.org class name.
        field_defs["type"] = (
            str,
            Field(default=class_def["schema_name"], alias="@type"),
        )

        model = create_model(
            class_name,
            __base__=base,
            __doc__=class_def["comment"] or f"schema.org/{class_name}",
            **field_defs,
        )
        cache[class_name] = model
        return model

    for class_name in classes:
        build(class_name)

    return {
        class_name: model for class_name, model in cache.items() if model is not None
    }


def _render_field(name: str, field_info: FieldInfo) -> str:
    """Render a single schema.org property field as a source code line."""
    source = field_info.json_schema_extra["source"]

    args = ["default=None"]
    if field_info.alias:
        args.append(f'alias="{field_info.alias}"')

    return f"{name}: {source} = Field({', '.join(args)})"


def _topological_sort(models: dict[str, type[BaseModel]]) -> list[str]:
    """Return model names sorted so every parent appears before its children."""
    visited: set[str] = set()
    order: list[str] = []

    def visit(class_name: str) -> None:
        if class_name in visited or class_name not in models:
            return
        visited.add(class_name)
        for base_cls in models[class_name].__bases__:
            visit(base_cls.__name__)
        order.append(class_name)

    for class_name in sorted(models):
        visit(class_name)

    return order


def render_module(models: dict[str, type[BaseModel]], strict: bool) -> str:
    """
    Render all models into a Python module string.

    Parameters
    ----------
    models : dict[str, type[BaseModel]]
        Built Pydantic model classes to render.
    strict : bool
        Recorded in the module header for documentation purposes.

    Returns
    -------
    str
        The full source of the generated Python module.
    """
    order = _topological_sort(models)
    generated_class_names = set(order)

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
        "from typing import Any, Optional",
        "",
        "from pydantic import AnyUrl, BaseModel, ConfigDict, Field",
        "",
        "",
        inspect.getsource(SchemaOrgBase),
        "",
    ]

    for class_name in order:
        cls = models[class_name]

        parent = next(
            (
                base_cls.__name__
                for base_cls in cls.__bases__
                if base_cls.__name__ in generated_class_names
            ),
            "SchemaOrgBase",
        )

        lines.append(f"class {class_name}({parent}):")

        doc = cls.__doc__ or f"schema.org/{class_name}"
        indented_doc = doc.replace("\n", "\n    ")
        lines.append(f'    """{indented_doc}"""')
        lines.append("")

        parent_cls = cls.__bases__[0]
        own_fields = {
            name: cls.model_fields[name]
            for name in cls.model_fields
            if name not in parent_cls.model_fields
        }

        # Always emit type explicitly — we know the correct schema.org name
        # (which may differ from the Python class name for renamed classes
        # like _3DModel) and every class must declare its own @type default.
        schema_name = cls.model_fields["type"].default
        lines.append(f'    type: str = Field(default="{schema_name}", alias="@type")')

        for field_name, field_info in own_fields.items():
            rendered = _render_field(field_name, field_info)
            lines.append(f"    {rendered}")

        lines.append("")

    lines.append("")
    lines.append("# Resolve forward references between models")
    lines.extend(f"{class_name}.model_rebuild()" for class_name in order)
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

    print(f"Building {len(classes)} Pydantic model classes ...")
    models = build_models(classes, class_fields, strict)

    print("Rendering source code...")
    source = render_module(models, strict)

    out.write_text(source, encoding="utf-8")
    print(f"Written to {out} ({len(models)} models)")


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
